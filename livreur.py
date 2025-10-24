#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import random
import threading
from uuid import uuid4

import redis

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
COURIER_ID = os.getenv("COURIER_ID", f"courier-{uuid4().hex[:6]}")

CHAN_ANNONCES = os.getenv("CHAN_ANNONCES", "annonces")
CHAN_CANDIDATURES = os.getenv("CHAN_CANDIDATURES", "candidatures")
CHAN_AFFECTATIONS = os.getenv("CHAN_AFFECTATIONS", "affectations")


def get_redis():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    return r


def _listen_affectations(r: redis.Redis):
    """Thread: écoute les affectations pour ce livreur et affiche si sélectionné."""
    p = r.pubsub()
    p.subscribe(CHAN_AFFECTATIONS)
    print(f"[{COURIER_ID}] ✅ Abonné aux affectations.", flush=True)
    try:
        for msg in p.listen():
            if msg.get("type") != "message":
                continue
            try:
                data = json.loads(msg["data"])
            except Exception:
                continue
            if data.get("courier_id") == COURIER_ID:
                order_id = data.get("order_id")
                print(f"[{COURIER_ID}] 🎯 Affecté sur la course {order_id} (ETA retenue: {data.get('eta_minutes')} min).", flush=True)
    finally:
        p.close()


def ecouter_annonces_et_postuler(r: redis.Redis):
    p = r.pubsub()
    p.subscribe(CHAN_ANNONCES)
    print(f"[{COURIER_ID}] 👂 En attente d'annonces sur '{CHAN_ANNONCES}'…", flush=True)

    try:
        for msg in p.listen():
            if msg.get("type") != "message":
                continue

            # Une annonce de course
            try:
                a = json.loads(msg["data"])
            except Exception:
                continue

            order_id = a.get("order_id")
            if not order_id:
                continue

            pickup = a.get("pickup", "?")
            dropoff = a.get("dropoff", "?")
            reward = a.get("reward_eur", 0)

            print(
                f"\n[{COURIER_ID}] 📣 Nouvelle course:"
                f"\n  - order_id : {order_id}"
                f"\n  - pickup   : {pickup}"
                f"\n  - dropoff  : {dropoff}"
                f"\n  - prime    : {reward} €",
                flush=True,
            )

            # Demande de confirmation
            rep = input("Accepter cette livraison ? [o/n] ").strip().lower()
            if rep == "o":
                candidature = {
                    "order_id": order_id,
                    "courier_id": COURIER_ID,
                    "eta_minutes": random.randint(4, 12),
                    "ts": time.time(),
                }
                r.publish(CHAN_CANDIDATURES, json.dumps(candidature))
                print(f"[{COURIER_ID}] 📨 Candidature envoyée: {candidature}", flush=True)

                # Enregistrer les gains du livreur après l'affectation
                enregistrer_gain_livreur(r, COURIER_ID, reward)

                # Afficher les gains du livreur
                afficher_gains_livreur(r, COURIER_ID)

            else:
                print(f"[{COURIER_ID}] ❌ Rejetée par le livreur.", flush=True)
    finally:
        p.close()


def enregistrer_gain_livreur(r, courier_id, reward_eur):
    """Enregistre les gains du livreur dans Redis."""
    r.hincrbyfloat(f"courier:{courier_id}:gains", "total", reward_eur)
    print(f"[LIVREUR] ✅ Gain ajouté pour {courier_id}: {reward_eur} €")


def afficher_gains_livreur(r, courier_id):
    """Affiche les gains du livreur."""
    total_gains = r.hget(f"courier:{courier_id}:gains", "total")
    print(f"[LIVREUR] 💰 Gains totaux pour {courier_id}: {total_gains} €")


def main():
    r = get_redis()
    print(f"[{COURIER_ID}] 🔗 Connecté à {REDIS_URL}", flush=True)

    # Thread d'écoute des affectations
    th = threading.Thread(target=_listen_affectations, args=(r,), daemon=True)
    th.start()

    # Boucle principale
    ecouter_annonces_et_postuler(r)


if __name__ == "__main__":
    main()

