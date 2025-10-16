#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import sys
import redis

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
RESTAURANT_INDEX = os.getenv("RESTAURANT_INDEX", "restaurants:index")


def get_redis():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    print(f"[LOADER] 🔗 Connecté à {REDIS_URL}")
    return r


def _first_non_empty(row, *candidates):
    for c in candidates:
        v = row.get(c)
        if v is not None:
            v = str(v).strip()
            if v != "":
                return v
    return None


def load_csv_to_redis(csv_path: str):
    r = get_redis()
    n = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Détecte un identifiant
            rid = _first_non_empty(row, "id", "restaurant_id", "business_id", "place_id", "identifier") or str(n)

            key = f"restaurant:{rid}"

            # Devine quelques champs utiles pour l’affichage
            name = _first_non_empty(row, "name", "restaurant_name", "business_name", "title")
            city = _first_non_empty(row, "city", "ville", "locality", "town")
            address = _first_non_empty(row, "address", "adresse", "street", "street_address")
            cuisine = _first_non_empty(row, "cuisine", "categories", "category", "food_type")
            lat = _first_non_empty(row, "latitude", "lat", "geo_lat")
            lon = _first_non_empty(row, "longitude", "lng", "lon", "geo_lon")
            rating = _first_non_empty(row, "rating", "stars", "review_score")

            # Écrit la ligne originale
            r.hset(key, mapping=row)

            # Ajoute des champs normalisés (prefixe _std_)
            std = {
                "_std_name": name or "",
                "_std_city": city or "",
                "_std_address": address or "",
                "_std_cuisine": cuisine or "",
                "_std_lat": lat or "",
                "_std_lon": lon or "",
                "_std_rating": rating or "",
            }
            r.hset(key, mapping=std)

            # Index pour tirage rapide
            r.sadd(RESTAURANT_INDEX, key)

            n += 1

    print(f"[LOADER] ✅ {n} restaurants chargés dans Redis.")
    print(f"[LOADER] 📚 Index disponible dans le set '{RESTAURANT_INDEX}'.")
    print("[LOADER] 💡 Astuce: tu peux vérifier par `SCARD restaurants:index` puis `SRANDMEMBER restaurants:index` dans redis-cli.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python load_kaggle_to_redis.py <chemin_du_csv>")
        sys.exit(1)
    load_csv_to_redis(sys.argv[1])


if __name__ == "__main__":
    main()

