#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, random
from uuid import uuid4
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

CHAN_COMMANDES    = os.getenv("CHAN_COMMANDES", "commandes")      # client -> manager
CHAN_ANNONCES     = os.getenv("CHAN_ANNONCES", "annonces")        # manager -> livreurs
CHAN_CANDIDATURES = os.getenv("CHAN_CANDIDATURES", "candidatures") # livreurs -> manager
CHAN_AFFECTATIONS = os.getenv("CHAN_AFFECTATIONS", "affectations") # manager -> livreur choisi

RESTAURANT_INDEX = os.getenv("RESTAURANT_INDEX", "restaurants:index")
MIN_REWARD = float(os.getenv("MIN_REWARD_EUR", "5.0"))
MAX_REWARD = float(os.getenv("MAX_REWARD_EUR", "10.0"))
AUTO_APPROVE = os.getenv("AUTO_APPROVE", "1") == "1"  # sinon, poserait une question (non interactif ici)

def get_redis():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    return r

def _pickup_display(r, restaurant_key):
    h = r.hgetall(restaurant_key)
    name = h.get("_std_name") or h.get("name") or restaurant_key
    city = h.get("_std_city") or h.get("city") or ""
    address = h.get("_std_address") or h.get("address") or ""
    pickup = name
    if city: pickup += f" ({city})"
    if address: pickup += f" · {address}"
    return pickup

def publier_annonce(r, order_id, pickup, dropoff, reward_eur, details_extra=None):
    annonce = {
        "order_id": order_id,
        "pickup": pickup,
        "dropoff": dropoff,
        "reward_eur": float(reward_eur),
        "ts": time.time(),
    }
    if details_extra:
        annonce["details"] = details_extra
    r.publish(CHAN_ANNONCES, json.dumps(annonce))
    print(f"[MANAGER] 📣 Annonce publiée: {annonce}")

def collecter_candidatures(r, order_id, timeout_s=30):
    p = r.pubsub()
    p.subscribe(CHAN_CANDIDATURES)
    print(f"[MANAGER] ⏳ Attente candidatures ({timeout_s}s) pour {order_id}…")
    pool, deadline = [], time.time()+timeout_s
    try:
        while time.time() < deadline:
            msg = p.get_message(timeout=1.0)
            if not msg or msg.get("type") != "message": continue
            try: c = json.loads(msg["data"])
            except Exception: continue
            if c.get("order_id") != order_id: continue
            pool.append(c)
            print(f"[MANAGER] ➕ Candidature: {c}")
    finally:
        p.close()
    print(f"[MANAGER] 🧮 Total candidatures: {len(pool)}")
    return pool

def choisir_et_affecter(r, order_id, candidatures):
    if not candidatures:
        print(f"[MANAGER] ⚠️ Aucune candidature pour {order_id}."); return None
    gagnant = min(candidatures, key=lambda x: x.get("eta_minutes", 1e9))
    affectation = {
        "order_id": order_id,
        "courier_id": gagnant.get("courier_id"),
        "eta_minutes": gagnant.get("eta_minutes"),
        "ts": time.time(),
    }
    r.publish(CHAN_AFFECTATIONS, json.dumps(affectation))
    r.set(f"order:{order_id}:assigned_to", affectation["courier_id"])
    r.set(f"order:{order_id}:eta_minutes", affectation["eta_minutes"])
    print(f"[MANAGER] 🏁 Affectation publiée: {affectation}")
    return affectation

def handle_commande(r, cmd):
    """Décide si on envoie aux livreurs. Ici: AUTO_APPROVE ou règle simple."""
    rid = cmd.get("order_request_id")
    restokey = cmd.get("restaurant_key")
    dish = cmd.get("dish")
    client_id = cmd.get("client_id")
    if not (rid and restokey and dish and client_id):
        print(f"[MANAGER] ❌ Commande invalide: {cmd}"); return

    # Décision (ex: 100% si AUTO_APPROVE, sinon 80% de chance)
    approve = AUTO_APPROVE or (random.random() < 0.8)
    print(f"[MANAGER] 🧠 Décision pour {rid}: {'APPROUVÉ' if approve else 'REFUSÉ'}")

    if not approve:
        r.set(f"order_req:{rid}:status", "rejected")
        return

    order_id = f"order-{uuid4().hex[:8]}"
    pickup = _pickup_display(r, restokey)
    dropoff = f"Client {client_id}"
    reward = round(random.uniform(MIN_REWARD, MAX_REWARD), 2)

    # Persiste quelques métadonnées
    r.hset(f"order:{order_id}", mapping={
        "client_id": client_id,
        "restaurant_key": restokey,
        "dish": dish,
        "status": "announced",
        "ts": time.time(),
    })

    publier_annonce(r, order_id, pickup, dropoff, reward_eur=reward,
                    details_extra={"dish": dish, "client_id": client_id, "order_request_id": rid})

    cands = collecter_candidatures(r, order_id, timeout_s=30)
    affectation = choisir_et_affecter(r, order_id, cands)

    # Enregistrement de la commande traitée pour la facturation
    if affectation:
        courier_id = affectation.get("courier_id")
        enregistrer_commande(r, order_id, restokey, courier_id, reward)

def enregistrer_commande(r, order_id, restaurant_key, courier_id, reward_eur):
    """Enregistre la commande traitée pour le suivi des gains."""
    r.hset(f"order:{order_id}", mapping={
        "restaurant_key": restaurant_key,
        "courier_id": courier_id,
        "reward_eur": reward_eur,
        "status": "completed",
        "ts": time.time(),
    })
    print(f"[MANAGER] ✅ Commande {order_id} enregistrée pour facturation.")

def calculer_gains(r):
    """Calcule les gains totaux des restaurants et des livreurs."""
    restaurants_gains = {}
    livreurs_gains = {}

    for key in r.scan_iter("order:*"):
        order_data = r.hgetall(key)
        if order_data.get("status") != "completed":
            continue

        restaurant_key = order_data.get("restaurant_key")
        courier_id = order_data.get("courier_id")
        reward_eur = float(order_data.get("reward_eur", 0))

        # Calcul des gains restaurant
        if restaurant_key in restaurants_gains:
            restaurants_gains[restaurant_key] += reward_eur
        else:
            restaurants_gains[restaurant_key] = reward_eur

        # Calcul des gains livreur
        if courier_id in livreurs_gains:
            livreurs_gains[courier_id] += reward_eur
        else:
            livreurs_gains[courier_id] = reward_eur

    # Affichage des résultats
    print("\n=== Gains des restaurants ===")
    for restaurant_key, total in restaurants_gains.items():
        print(f"Restaurant {restaurant_key}: {total:.2f} €")

    print("\n=== Gains des livreurs ===")
    for courier_id, total in livreurs_gains.items():
        print(f"Livreur {courier_id}: {total:.2f} €")

def fin_de_journee():
    r = get_redis()
    calculer_gains(r)

def listen_loop():
    r = get_redis()
    print(f"[MANAGER] Connecté à {REDIS_URL}")
    p = r.pubsub()
    p.subscribe(CHAN_COMMANDES)
    print(f"[MANAGER] 👂 En attente de commandes client sur '{CHAN_COMMANDES}'…")
    try:
        for msg in p.listen():
            if msg.get("type") != "message": continue
            try: cmd = json.loads(msg["data"])
            except Exception: 
                print("[MANAGER] Message non-JSON ignoré."); continue
            print(f"[MANAGER] 📨 Commande reçue: {cmd}")
            handle_commande(r, cmd)
    finally:
        p.close()

    # Appel à la fin de la journée
    fin_de_journee()  # Cette ligne va afficher les gains dans la CLI

if __name__ == "__main__":
    listen_loop()

