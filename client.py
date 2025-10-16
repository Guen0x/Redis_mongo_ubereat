#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, random
from uuid import uuid4
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
RESTAURANT_INDEX = os.getenv("RESTAURANT_INDEX", "restaurants:index")
CHAN_COMMANDES = os.getenv("CHAN_COMMANDES", "commandes")

CLIENT_ID = os.getenv("CLIENT_ID", f"client-{uuid4().hex[:6]}")

CUISINE_TO_DISHES = {
    "italian": ["Margherita", "Carbonara", "Lasagne", "Penne Arrabiata"],
    "pizza": ["Margherita", "Diavola", "4 Fromages", "Regina"],
    "japanese": ["Sushi Mix", "Ramen Shoyu", "Ramen Miso", "Donburi"],
    "chinese": ["Poulet croustillant", "Nouilles sautées", "Canard laqué", "Riz cantonais"],
    "thai": ["Pad Thaï", "Green Curry", "Tom Yum", "Basilic sauté"],
    "indian": ["Butter Chicken", "Tikka Masala", "Biryani", "Dal"],
    "lebanese": ["Chawarma", "Mezzé", "Falafel", "Taboulé"],
    "greek": ["Gyros", "Moussaka", "Souvlaki", "Salade grecque"],
    "mexican": ["Tacos", "Burrito", "Quesadilla", "Chili con carne"],
    "burger": ["Cheeseburger", "Bacon Burger", "Veggie Burger", "Double"],
    "french": ["Boeuf bourguignon", "Quiche", "Croque-monsieur", "Salade niçoise"],
}

def get_redis():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True); r.ping(); return r

def _random_restaurant_keys(r, k=5):
    # Prend jusqu'à k restos aléatoires depuis l'index
    keys = []
    size = r.scard(RESTAURANT_INDEX)
    if size and size > 0:
        for _ in range(min(k, size)):
            m = r.srandmember(RESTAURANT_INDEX)
            if m and m not in keys:
                keys.append(m)
    else:
        # fallback : scan
        for i, key in enumerate(r.scan_iter("restaurant:*", count=200)):
            keys.append(key)
            if len(keys) >= k:
                break
    return keys

def _menu_for_restaurant(h):
    # Essaie de lire un menu déjà stocké
    menu_key = h.get("_menu_key")
    r = get_redis()
    if menu_key and r.exists(menu_key):
        return menu_key, r.lrange(menu_key, 0, -1)

    # Sinon, crée un menu à partir de la cuisine
    cuisine = (h.get("_std_cuisine") or h.get("cuisine") or "").lower()
    base = []
    for key, dishes in CUISINE_TO_DISHES.items():
        if key in cuisine:
            base = dishes; break
    if not base:
        base = ["Plat du jour", "Salade composée", "Pâtes", "Dessert maison"]

    menu_key = f"{h.get('_std_name','restaurant')}:{uuid4().hex[:6]}:menu"
    r.rpush(menu_key, *base)
    r.hset(h["_redis_key"], mapping={"_menu_key": menu_key})
    return menu_key, base

def choose_and_send():
    r = get_redis()
    print(f"[CLIENT {CLIENT_ID}] Connecté à {REDIS_URL}\n")
    keys = _random_restaurant_keys(r, k=5)
    if not keys:
        print("❌ Aucun restaurant trouvé. Charge d’abord ton CSV avec load_kaggle_to_redis.py")
        return

    restos = []
    print("=== Choisir un restaurant ===")
    for i, k in enumerate(keys, 1):
        h = r.hgetall(k)
        name = h.get("_std_name") or h.get("name") or k
        city = h.get("_std_city") or h.get("city") or ""
        line = f"{i}. {name}" + (f" ({city})" if city else "")
        print(line)
        h["_redis_key"] = k
        restos.append(h)

    idx = input("Numéro du restaurant: ").strip()
    try:
        idx = int(idx); assert 1 <= idx <= len(restos)
    except Exception:
        print("Choix invalide."); return
    resto = restos[idx-1]
    resto_name = resto.get("_std_name") or resto.get("name")

    menu_key, dishes = _menu_for_restaurant(resto)
    print(f"\n=== Menu de {resto_name} ===")
    for i, d in enumerate(dishes, 1):
        print(f"{i}. {d}")
    dsel = input("Numéro du plat: ").strip()
    try:
        dsel = int(dsel); assert 1 <= dsel <= len(dishes)
    except Exception:
        print("Choix invalide."); return
    dish = dishes[dsel-1]

    # Compose et envoie la commande
    commande = {
        "order_request_id": f"req-{uuid4().hex[:8]}",
        "restaurant_key": resto["_redis_key"],
        "restaurant_name": resto_name,
        "dish": dish,
        "client_id": CLIENT_ID,
        "ts": time.time(),
    }
    r.publish(CHAN_COMMANDES, json.dumps(commande))
    print(f"\n[CLIENT {CLIENT_ID}] 📨 Commande envoyée au manager: {commande}")

if __name__ == "__main__":
    choose_and_send()

