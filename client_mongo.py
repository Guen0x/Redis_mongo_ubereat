#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, random, sys
from uuid import uuid4
from pymongo import MongoClient
from bson.objectid import ObjectId # Pour utiliser les _id

# --- Configuration ---
# MIS À JOUR : Utilisation de votre chaîne de connexion Atlas
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://stephane2yanis7_db_user:F5WHgcchyl1JeZYu@cluster0.0jxoseg.mongodb.net/?appName=Cluster0")
MONGO_DB = os.getenv("MONGO_DB", "ubereats_poc") 
RESTAURANT_COLLECTION = "restaurants"
COMMANDES_COLLECTION = "commandes" # Collection où le client écrit

CLIENT_ID = os.getenv("CLIENT_ID", f"client-{uuid4().hex[:6]}")

CUISINE_TO_DISHES = {
    "italian": ["Margherita", "Carbonara", "Lasagne", "Penne Arrabiata"],
    "pizza": ["Margherita", "Diavola", "4 Fromages", "Regina"],
    "japanese": ["Sushi Mix", "Ramen Shoyu", "Ramen Miso", "Donburi"],
    # ... (reste du dict CUISINE_TO_DISHES) ...
    "burger": ["Cheeseburger", "Bacon Burger", "Veggie Burger", "Double"],
    "french": ["Boeuf bourguignon", "Quiche", "Croque-monsieur", "Salade niçoise"],
}

def get_mongo():
    # MIS À JOUR : Pas de 'replicaSet' nécessaire avec 'srv'
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    # Teste la connexion
    try:
        db.command("ping")
    except Exception as e:
        print(f"[CLIENT {CLIENT_ID}] ❌ Échec de la connexion à Atlas: {e}")
        print("   -> Avez-vous bien remplacé 'VOTRE_MOT_DE_PASSE_ICI' ?")
        print("   -> Avez-vous bien autorisé votre adresse IP sur MongoDB Atlas ?")
        sys.exit(1)
    return db

def _random_restaurants(db, k=5):
    # Utilise $sample pour prendre k restos aléatoires
    pipeline = [{"$sample": {"size": k}}]
    restos = list(db[RESTAURANT_COLLECTION].aggregate(pipeline))
    return restos

def _menu_for_restaurant(db, resto_doc):
    # Essaie de lire un menu déjà stocké dans le document
    if "menu" in resto_doc and resto_doc["menu"]:
        return resto_doc["menu"]

    # Sinon, crée un menu à partir de la cuisine
    cuisine = (resto_doc.get("_std_cuisine") or "").lower()
    base = []
    for key, dishes in CUISINE_TO_DISHES.items():
        if key in cuisine:
            base = dishes; break
    if not base:
        base = ["Plat du jour", "Salade composée", "Pâtes", "Dessert maison"]

    # Met à jour le document resto avec ce menu pour la postérité
    db[RESTAURANT_COLLECTION].update_one(
        {"_id": resto_doc["_id"]},
        {"$set": {"menu": base}}
    )
    return base

def choose_and_send():
    db = get_mongo()
    print(f"[CLIENT {CLIENT_ID}] Connecté à MongoDB Atlas\n")
    
    restos = _random_restaurants(db, k=5)
    if not restos:
        print(f"❌ Aucun restaurant trouvé dans la collection '{RESTAURANT_COLLECTION}'.")
        print("   Charge d’abord ton CSV avec load_kaggle_to_mongo.py")
        return

    print("=== Choisir un restaurant ===")
    for i, h in enumerate(restos, 1):
        name = h.get("_std_name") or h.get("original_data", {}).get("name") or str(h["_id"])
        city = h.get("_std_city") or ""
        line = f"{i}. {name}" + (f" ({city})" if city else "")
        print(line)

    idx = input("Numéro du restaurant: ").strip()
    try:
        idx = int(idx); assert 1 <= idx <= len(restos)
    except Exception:
        print("Choix invalide."); return
    
    resto = restos[idx-1]
    resto_name = resto.get("_std_name") or resto.get("original_data", {}).get("name")

    dishes = _menu_for_restaurant(db, resto)
    print(f"\n=== Menu de {resto_name} ===")
    for i, d in enumerate(dishes, 1):
        print(f"{i}. {d}")
    dsel = input("Numéro du plat: ").strip()
    try:
        dsel = int(dsel); assert 1 <= dsel <= len(dishes)
    except Exception:
        print("Choix invalide."); return
    dish = dishes[dsel-1]

    # Compose et envoie la commande en l'insérant dans la collection
    commande = {
        "order_request_id": f"req-{uuid4().hex[:8]}",
        "restaurant_id": resto["_id"], # Utilise l'ObjectId de Mongo
        "restaurant_name": resto_name,
        "dish": dish,
        "client_id": CLIENT_ID,
        "status": "pending", # Statut initial de la *demande*
        "ts": time.time(),
    }
    
    insert_result = db[COMMANDES_COLLECTION].insert_one(commande)
    print(f"\n[CLIENT {CLIENT_ID}] 📨 Commande envoyée au manager (ID: {insert_result.inserted_id})")

if __name__ == "__main__":
    choose_and_send()