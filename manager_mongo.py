#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, random, sys
from uuid import uuid4
from pymongo import MongoClient
from bson.objectid import ObjectId

# --- Configuration ---
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://stephane2yanis7_db_user:F5WHgcchyl1JeZYu@cluster0.0jxoseg.mongodb.net/?appName=Cluster0")
MONGO_DB = os.getenv("MONGO_DB", "ubereats_poc") 

# Collections
COMMANDES_COLLECTION = "commandes"  # client -> manager
ORDERS_COLLECTION = "orders"        # manager -> livreurs (via Change Stream)
CANDIDATURES_COLLECTION = "candidatures" # livreurs -> manager
RESTAURANT_COLLECTION = "restaurants"

# Logique métier
MIN_REWARD = float(os.getenv("MIN_REWARD_EUR", "5.0"))
MAX_REWARD = float(os.getenv("MAX_REWARD_EUR", "10.0"))
AUTO_APPROVE = os.getenv("AUTO_APPROVE", "1") == "1"
CANDIDATURE_TIMEOUT_S = 30 # Temps d'attente pour les candidatures

def get_mongo():
    # MIS À JOUR : Suppression de 'replicaSet="rs0"'
    client = MongoClient(MONGO_URL) 
    db = client[MONGO_DB]
    # Teste la connexion
    try:
        db.command("ping")
        print(f"[MANAGER] 🔗 Connecté à MongoDB Atlas (DB: {MONGO_DB})")
    except Exception as e:
        print(f"[MANAGER] ❌ Échec de la connexion à Atlas: {e}")
        print("   -> Avez-vous bien remplacé 'VOTRE_MOT_DE_PASSE_ICI' ?")
        print("   -> Avez-vous bien autorisé votre adresse IP sur MongoDB Atlas ?")
        sys.exit(1)
    return db

def _pickup_display(db, restaurant_id):
    h = db[RESTAURANT_COLLECTION].find_one({"_id": restaurant_id})
    if not h: return str(restaurant_id)
    
    name = h.get("_std_name") or h.get("original_data", {}).get("name") or str(restaurant_id)
    city = h.get("_std_city") or ""
    address = h.get("_std_address") or ""
    pickup = name
    if city: pickup += f" ({city})"
    if address: pickup += f" · {address}"
    return pickup

def collecter_candidatures(db, order_id, timeout_s=30):
    print(f"[MANAGER] ⏳ Attente candidatures ({timeout_s}s) pour {order_id}…")
    # Logique la plus simple : attendre N secondes puis collecter
    time.sleep(timeout_s)
    
    pool = list(db[CANDIDATURES_COLLECTION].find({"order_id": order_id}))
    
    print(f"[MANAGER] 🧮 Total candidatures: {len(pool)}")
    return pool

# --- FONCTION MODIFIÉE ---
def choisir_et_affecter(db, order_id, candidatures):
    
    gagnant_data = {} # Stockera l'ID et l'ETA du gagnant

    if not candidatures:
        # --- MODIFIÉ ---
        # S'il n'y a AUCUNE candidature, affecter au livreur permanent
        print(f"[MANAGER] ⚠️ Aucune candidature pour {order_id}.")
        print(f"[MANAGER] 🤖 Affectation automatique au livreur permanent 'coursier-permanent-001'.")
        gagnant_data = {
            "coursier_id": "coursier-permanent-001",
            "eta_minutes": 15 # ETA par défaut pour le livreur interne
        }
    else:
        # --- MODIFIÉ ---
        # Logique normale : choisir le meilleur ETA
        print(f"[MANAGER] ✅ {len(candidatures)} candidature(s) reçue(s). Choix du meilleur ETA.")
        gagnant_obj = min(candidatures, key=lambda x: x.get("eta_minutes", 1e9))
        gagnant_data = {
            "coursier_id": gagnant_obj.get("cousier_id"),
            "eta_minutes": gagnant_obj.get("eta_minutes")
        }
    
    # --- MODIFIÉ ---
    # Logique d'affectation (commune aux deux cas)
    affectation_data = {
        "coursier_id": gagnant_data.get("coursier_id"),
        "eta_minutes": gagnant_data.get("eta_minutes"),
        "status": "assigned", # Mise à jour du statut
        "ts_assigned": time.time()
    }
    
    # L'affectation est une simple MISE A JOUR du document 'order'
    result = db[ORDERS_COLLECTION].update_one(
        {"_id": order_id},
        {"$set": affectation_data}
    )
    
    if result.modified_count > 0:
        print(f"[MANAGER] 🏁 Affectation publiée (MAJ Ordre {order_id}): {gagnant_data.get('coursier_id')}")
        return affectation_data
    else:
        print(f"[MANAGER] ❌ Erreur lors de l'affectation de {order_id}.")
        return None

def handle_commande(db, cmd):
    """Décide si on envoie aux livreurs."""
    req_id = cmd.get("_id") # ID de la *demande*
    restaurant_id = cmd.get("restaurant_id")
    dish = cmd.get("dish")
    client_id = cmd.get("client_id")
    if not (req_id and restaurant_id and dish and client_id):
        print(f"[MANAGER] ❌ Commande invalide: {cmd}"); return

    # Décision
    approve = AUTO_APPROVE or (random.random() < 0.8)
    print(f"[MANAGER] 🧠 Décision pour {req_id}: {'APPROUVÉ' if approve else 'REFUSÉ'}")

    # Met à jour la *demande* de commande (feedback pour le client)
    db[COMMANDES_COLLECTION].update_one(
        {"_id": req_id},
        {"$set": {"status": "approved" if approve else "rejected"}}
    )
    if not approve:
        return

    # Création de l'ordre de mission (la "course")
    order_id = f"order-{uuid4().hex[:8]}"
    pickup = _pickup_display(db, restaurant_id)
    dropoff = f"Client {client_id}"
    reward = round(random.uniform(MIN_REWARD, MAX_REWARD), 2)

    # L'annonce est une simple INSERTION dans la collection 'orders'
    # Le Change Stream du livreur réagira à cet 'insert'
    order_doc = {
        "_id": order_id,
        "request_id": req_id, # Lien vers la demande client
        "restaurant_id": restaurant_id,
        "pickup": pickup,
        "dropoff": dropoff,
        "reward_eur": float(reward),
        "dish": dish,
        "client_id": client_id,
        "status": "announced", # Statut initial
        "ts_created": time.time(),
    }
    db[ORDERS_COLLECTION].insert_one(order_doc)
    print(f"[MANAGER] 📣 Annonce publiée (via INSERT Ordre {order_id})")

    # Lancer le processus de collecte
    cands = collecter_candidatures(db, order_id, timeout_s=CANDIDATURE_TIMEOUT_S)
    affectation = choisir_et_affecter(db, order_id, cands)
    
    # (La logique d'enregistrement des gains est gérée par 'calculer_gains')


def calculer_gains(db):
    """Calcule les gains totaux en utilisant des agrégations MongoDB."""
    print("\n--- FIN DE JOURNÉE: CALCUL DES GAINS ---")

    # === Gains des livreurs ===
    pipeline_livreurs = [
        {"$match": {"status": "assigned", "coursier_id": {"$exists": True}}},
        {"$group": {
            "_id": "$coursier_id",
            "total_gains": {"$sum": "$reward_eur"},
            "courses": {"$sum": 1}
        }},
        {"$sort": {"total_gains": -1}}
    ]
    print("\n=== Gains des livreurs ===")
    livreurs_gains = db[ORDERS_COLLECTION].aggregate(pipeline_livreurs)
    for gain in livreurs_gains:
        print(f"Livreur {gain['_id']}: {gain['total_gains']:.2f} € ({gain['courses']} courses)")

    # === Gains (Chiffre d'affaires) des restaurants ===
    pipeline_restos = [
        {"$match": {"status": "assigned"}},
        {"$group": {
            "_id": "$restaurant_id",
            "total_CA": {"$sum": "$reward_eur"}, # Note: c'est la prime, pas le CA réel
            "courses": {"$sum": 1}
        }},
        {"$sort": {"total_CA": -1}}
    ]
    print("\n=== CA par Restaurant (côté livraison) ===")
    restos_gains = db[ORDERS_COLLECTION].aggregate(pipeline_restos)
    for gain in restos_gains:
        # Tente de récupérer le nom du resto pour un meilleur affichage
        resto_name = _pickup_display(db, gain['_id'])
        print(f"Restaurant {resto_name}: {gain['total_CA']:.2f} € ({gain['courses']} courses)")
    print("-------------------------------------------\n")


def listen_loop():
    db = get_mongo()
    
    # Pipeline pour ne voir que les NOUVELLES commandes client
    pipeline = [{"$match": {"operationType": "insert"}}]
    
    print(f"[MANAGER] 👂 En attente de commandes client sur '{COMMANDES_COLLECTION}'… (via Change Stream)")
    
    try:
        # 'watch' est une boucle bloquante, comme p.listen()
        with db[COMMANDES_COLLECTION].watch(pipeline=pipeline) as stream:
            for change in stream:
                cmd = change["fullDocument"]
                print(f"[MANAGER] 📨 Commande reçue: {cmd.get('_id')}")
                # Gérer la commande (dans un thread serait mieux, mais restons simple)
                handle_commande(db, cmd)
    except KeyboardInterrupt:
        print("\n[MANAGER] Arrêt demandé.")
    except Exception as e:
        print(f"[MANAGER] Erreur de Change Stream: {e}")
        print("   -> Assurez-vous que votre cluster Atlas est bien un Replica Set (c'est le cas par défaut).")
    finally:
        print("[MANAGER] Arrêt du listener.")
        # Appel à la fin de la journée
        calculer_gains(db)

if __name__ == "__main__":
    listen_loop()