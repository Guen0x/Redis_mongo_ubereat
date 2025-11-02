#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import sys
from pymongo import MongoClient

# --- Configuration ---
# MIS À JOUR : Utilisation de votre chaîne de connexion Atlas
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://stephane2yanis7_db_user:F5WHgcchyl1JeZYu@cluster0.0jxoseg.mongodb.net/?appName=Cluster0")
MONGO_DB = os.getenv("MONGO_DB", "ubereats_poc") # Vous pouvez garder ce nom ou le changer
RESTAURANT_COLLECTION = "restaurants"


def get_mongo():
    # MIS À JOUR : Suppression de 'replicaSet="rs0"'
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    # Teste la connexion
    try:
        db.command("ping")
        print(f"[LOADER] 🔗 Connecté à MongoDB Atlas (DB: {MONGO_DB})")
    except Exception as e:
        print(f"[LOADER] ❌ Échec de la connexion à Atlas: {e}")
        print("   -> Avez-vous bien remplacé 'VOTRE_MOT_DE_PASSE_ICI' ?")
        print("   -> Avez-vous bien autorisé votre adresse IP sur MongoDB Atlas ?")
        sys.exit(1)
    return db


def _first_non_empty(row, *candidates):
    for c in candidates:
        v = row.get(c)
        if v is not None:
            v = str(v).strip()
            if v != "":
                return v
    return None


def load_csv_to_mongo(csv_path: str):
    db = get_mongo()
    collection = db[RESTAURANT_COLLECTION]
    
    # Vider la collection pour un chargement propre
    collection.delete_many({})
    
    n = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        restaurants_to_load = []
        for row in reader:
            # Devine quelques champs utiles pour l’affichage
            name = _first_non_empty(row, "name", "restaurant_name", "business_name", "title")
            city = _first_non_empty(row, "city", "ville", "locality", "town")
            address = _first_non_empty(row, "address", "adresse", "street", "street_address")
            cuisine = _first_non_empty(row, "cuisine", "categories", "category", "food_type")
            lat = _first_non_empty(row, "latitude", "lat", "geo_lat")
            lon = _first_non_empty(row, "longitude", "lng", "lon", "geo_lon")
            rating = _first_non_empty(row, "rating", "stars", "review_score")

            # Création du document MongoDB
            doc = {
                # _id est généré par défaut, pas besoin de 'rid'
                "_std_name": name or "",
                "_std_city": city or "",
                "_std_address": address or "",
                "_std_cuisine": cuisine or "",
                "_std_lat": lat or "",
                "_std_lon": lon or "",
                "_std_rating": rating or "",
                "original_data": row # Stocke la ligne originale
            }
            restaurants_to_load.append(doc)
            n += 1

        # Insertion en masse
        if restaurants_to_load:
            collection.insert_many(restaurants_to_load)

    # Créer des index pour les recherches futures
    collection.create_index([("_std_name", 1)])
    collection.create_index([("_std_city", 1)])
    
    print(f"[LOADER] ✅ {n} restaurants chargés dans MongoDB (Collection: '{RESTAURANT_COLLECTION}').")
    print("[LOADER] 💡 Astuce: tu peux vérifier avec `db.restaurants.countDocuments()` puis `db.restaurants.findOne()` dans mongosh.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python load_kaggle_to_mongo.py <chemin_du_csv>")
        sys.exit(1)
    load_csv_to_mongo(sys.argv[1])


if __name__ == "__main__":
    main()