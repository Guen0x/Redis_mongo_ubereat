#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import random
import threading
import sys
from uuid import uuid4
from pymongo import MongoClient

# --- Configuration ---
# MIS À JOUR : Utilisation de votre chaîne de connexion Atlas
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://stephane2yanis7_db_user:F5WHgcchyl1JeZYu@cluster0.0jxoseg.mongodb.net/?appName=Cluster0")
MONGO_DB = os.getenv("MONGO_DB", "ubereats_poc") 
COURSIER_ID = os.getenv("COURSIER_ID", f"coursier-{uuid4().hex[:6]}")

# Collections
ORDERS_COLLECTION = "orders"        # Écoute les 'insert' (annonces) et 'update' (affectations)
CANDIDATURES_COLLECTION = "candidatures" # Écrit les candidatures
GAINS_COLLECTION = "coursier_gains"  # Stocke les gains


def get_mongo():
    # MIS À JOUR : Suppression de 'replicaSet="rs0"'
    client = MongoClient(MONGO_URL) 
    db = client[MONGO_DB]
    # Teste la connexion
    try:
        db.command("ping")
    except Exception as e:
        print(f"[{COURSIER_ID}] ❌ Échec de la connexion à Atlas: {e}")
        print("   -> Avez-vous bien remplacé 'VOTRE_MOT_DE_PASSE_ICI' ?")
        print("   -> Avez-vous bien autorisé votre adresse IP sur MongoDB Atlas ?")
        sys.exit(1)
    return db


def _listen_affectations(db: MongoClient):
    """Thread: écoute les affectations pour CE livreur."""
    
    # Pipeline: Ne réagir qu'aux MAJ, 
    # où le statut devient 'assigned'
    # ET où le coursier_id est le nôtre
    pipeline = [
        {"$match": {
            "operationType": "update",
            "updateDescription.updatedFields.status": "assigned",
            "fullDocument.coursier_id": COURSIER_ID
        }}
    ]
    
    print(f"[{COURSIER_ID}] ✅ Abonné aux affectations.", flush=True)
    try:
        with db[ORDERS_COLLECTION].watch(pipeline=pipeline) as stream:
            for change in stream:
                data = change["fullDocument"]
                order_id = data.get("_id")
                eta = data.get('eta_minutes')
                reward = data.get('reward_eur', 0)
                
                print(f"\n[{COURSIER_ID}] 🎯 Affecté sur la course {order_id} (ETA: {eta} min, Gain: {reward} €).", flush=True)
                
                # Enregistrer les gains
                enregistrer_gain_livreur(db, COURSIER_ID, reward)
                afficher_gains_livreur(db, COURSIER_ID)

    except Exception as e:
        print(f"[{COURSIER_ID}] Erreur stream affectations: {e}", flush=True)


def ecouter_annonces_et_postuler(db: MongoClient):
    """Boucle principale: écoute les annonces (insert) et postule (insert)."""
    
    # Pipeline: Ne réagir qu'aux NOUVELLES courses,
    # au statut 'announced'
    pipeline = [
        {"$match": {
            "operationType": "insert",
            "fullDocument.status": "announced"
        }}
    ]
    
    print(f"[{COURSIER_ID}] 👂 En attente d'annonces sur '{ORDERS_COLLECTION}'…", flush=True)

    try:
        with db[ORDERS_COLLECTION].watch(pipeline=pipeline) as stream:
            for change in stream:
                # Une nouvelle annonce de course
                a = change["fullDocument"]

                order_id = a.get("_id")
                if not order_id:
                    continue

                pickup = a.get("pickup", "?")
                dropoff = a.get("dropoff", "?")
                reward = a.get("reward_eur", 0)

                print(
                    f"\n[{COURSIER_ID}] 📣 Nouvelle course:"
                    f"\n  - order_id : {order_id}"
                    f"\n  - pickup   : {pickup}"
                    f"\n  - dropoff  : {dropoff}"
                    f"\n  - prime    : {reward} €",
                    flush=True,
                )

                # Demande de confirmation
                # Note: input() bloque le thread, ce qui est ok pour un POC
                rep = input(f"[{COURSIER_ID}] Accepter cette livraison ? [o/n] ").strip().lower()
                
                if rep == "o":
                    candidature = {
                        "_id": f"cand-{order_id}-{COURSIER_ID}", # ID unique
                        "order_id": order_id,
                        "coursier_id": COURSIER_ID,
                        "eta_minutes": random.randint(4, 12),
                        "ts": time.time(),
                    }
                    # Postuler = Insérer un document candidature
                    try:
                        db[CANDIDATURES_COLLECTION].insert_one(candidature)
                        print(f"[{COURSIER_ID}] 📨 Candidature envoyée pour {order_id}", flush=True)
                    except Exception as e:
                        if "duplicate key" in str(e):
                            print(f"[{COURSIER_ID}] ⚠️ Déjà postulé pour {order_id}", flush=True)
                        else:
                            print(f"[{COURSIER_ID}] ❌ Erreur candidature: {e}", flush=True)
                else:
                    print(f"[{COURSIER_ID}] ❌ Course {order_id} rejetée.", flush=True)
                    
    except KeyboardInterrupt:
        print(f"\n[{COURSIER_ID}] Arrêt.")
    except Exception as e:
        print(f"[{COURSIER_ID}] Erreur stream annonces: {e}", flush=True)
        print("   -> Assurez-vous que votre cluster Atlas est bien un Replica Set (c'est le cas par défaut).")


def enregistrer_gain_livreur(db, coursier_id, reward_eur):
    """Enregistre les gains du livreur dans une collection dédiée."""
    db[GAINS_COLLECTION].update_one(
        {"_id": coursier_id},
        {"$inc": {"total_gains": reward_eur, "total_courses": 1}},
        upsert=True # Crée le document s'il n'existe pas
    )
    print(f"[{COURSIER_ID}] ✅ Gain ajouté: {reward_eur} €", flush=True)

def afficher_gains_livreur(db, coursier_id):
    """Affiche les gains totaux du livreur."""
    gains_doc = db[GAINS_COLLECTION].find_one({"_id": coursier_id})
    if gains_doc:
        total = gains_doc.get("total_gains", 0)
        courses = gains_doc.get("total_courses", 0)
        print(f"[{COURSIER_ID}] 💰 Gains totaux: {total:.2f} € ({courses} courses)", flush=True)

def main():
    db = get_mongo()
    print(f"[{COURSIER_ID}] 🔗 Connecté à MongoDB Atlas", flush=True)
    afficher_gains_livreur(db, COURSIER_ID) # Afficher les gains au démarrage

    # Thread d'écoute des affectations (me concerne)
    th = threading.Thread(target=_listen_affectations, args=(db,), daemon=True)
    th.start()

    # Boucle principale (écoute des annonces pour tous)
    ecouter_annonces_et_postuler(db)


if __name__ == "__main__":
    main()