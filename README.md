# 🍕 Redis & MongoDB UberEat Clone

Un système de livraison de repas distribué en temps réel utilisant Redis pour la communication et MongoDB pour la persistance des données.

## 📋 Description

Ce projet est une implémentation Python d'un système de livraison de repas inspiré d'UberEats, utilisant une architecture distribuée avec :
- **Redis** : Communication en temps réel et cache
- **MongoDB** : Persistance des données restaurants
- **Architecture microservices** : Composants indépendants et scalables

## 🏗️ Architecture

```mermaid
graph TD
    A[Client] -->|Commande| B[Redis Pub/Sub]
    B --> C[Manager]
    C -->|Annonce| B
    D[Livreur 1] <-->|Candidature/Affectation| B
    E[Livreur 2] <-->|Candidature/Affectation| B
    F[Livreur N] <-->|Candidature/Affectation| B
    C -->|Stockage| G[(MongoDB)]
    C -->|Cache| H[(Redis)]

## 🚀 Fonctionnalités

- ✅ **Système client** : Interface en ligne de commande pour passer des commandes
- ✅ **Gestion des restaurants** : Chargement depuis CSV avec menu dynamique
- ✅ **Système de matching** : Attribution automatique des livreurs
- ✅ **Communication temps réel** : Architecture publish/subscribe
- ✅ **Suivi des gains** : Calcul des revenus pour restaurants et livreurs
- ✅ **Support multi-livreurs** : Scalabilité horizontale

## 🛠️ Prérequis

- Python 3.8+
- Redis 5.0+
- MongoDB 4.4+ (ou MongoDB Atlas)
- Fichier CSV avec données restaurants

2. **Installer les dépendances**
```bash
pip install -r requirements.txt
## 📦 Installation

1. **Cloner le repository**
```bash
git clone https://github.com/Guen0x/Redis_mongo_ubereat.git
cd Redis_mongo_ubereat
