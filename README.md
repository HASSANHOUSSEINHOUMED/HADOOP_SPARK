# 🏭 HADOOP_SPARK - E-Commerce Data Pipeline

---

## 📌 Contexte

**Projet** : Pipeline Big Data complet pour plateforme e-commerce **ShopNow+**  
**Matière** : Architecture Big Data (Hadoop, Spark, Kafka)  
**Type** : Projet en équipe (3 personnes)  
**Stack** : MERN + Kafka + HDFS + Spark  

Application e-commerce full-stack avec **pipeline de données en temps réel** pour analyser les comportements clients, gérer les stocks et calculer des KPIs métier.

---

## 👥 Équipe & Contributions

| Rôle | Personne | Spécialité |
|------|----------|-----------|
| **Front & Back** | Amaury TISSOT | React/Express - API REST |
| **Kafka** | Léa DRUFFIN | Streaming et intégration événementielle |
| **HDFS & Spark** 🔥 | **Hassan HOUSSEIN HOUMED** | Architecture données + KPIs analytiques |

---

## 🎯 Mon Rôle : Data Engineering avec Hadoop & Spark

J'ai conçu et implémenté **l'infrastructure Big Data complète** de ShopNow+ :

### 🏗️ Architecture HDFS (3 couches)

```
/user/spark/kafka_stream/
├── brut/                          # Données brutes Kafka
│   └── events/ → [Parquet]
├── curated/                       # Données filtrées par type d'événement
│   ├── view_product/
│   └── add_to_cart/
└── indicators/                    # KPIs finaux (dashboards)
    ├── top_viewed_products/
    ├── top_bought_products/
    ├── daily_revenue/
    ├── stock_alerts/
    └── global_stats/
```

**Logique** : 3 étapes de transformation (brut → curated → indicateurs) pour maintenir données propres et traçabilité.

---

### ⚙️ Spark Jobs : Du Streaming au Batch

#### **Job 1 : Spark Streaming** (Ingestion)
- Consomme événements Kafka (`VIEW_PRODUCT`, `ADD_TO_CART`)
- Sauvegarde en continu dans `/brut/events/` (format Parquet)
- Enrichissement et nettoyage

#### **Job 2 : Spark Batch** (Analytique)
- Exécution quotidienne (batch mode)
- Calcule 6 KPIs métier à partir des données brutes
- Génère résultats dans `/indicators/` pour visualisations

---

## 📊 KPIs Métier Implémentés

J'ai mis en place **6 indicateurs clés** pour piloter ShopNow+ :

| KPI | Description | Valeur |
|-----|-------------|--------|
| **Top 10 Produits Vus** | Produits les plus consultés | Rang produits populaires |
| **Top 10 Produits Achetés + CA** | Rentabilité réelle | Chiffre d'affaires par produit |
| **CA par Jour** | Tendance ventes | 346 620€ total / pic 124 650€ |
| **Alertes Rupture Stock** | Produits à réapprovisionner | 7 produits en alerte |
| **Produits par Gamme de Prix** | Stratégie tarifaire | Distribution par prix |
| **Statistiques Globales** | Santé plateforme | 2574 événements, **39.6% taux conversion** |

**Résultats clés** :
- 1844 consultations → 730 achats = **excellent taux de conversion (39.6%)**
- 7 produits en rupture de stock détectés automatiquement
- Pipeline robuste : 0 perte de données, format Parquet compression 5-10×

---

## 🔄 Flux de Données Complet

```
Frontend (React)
    ↓ [événements: VIEW_PRODUCT, ADD_TO_CART]
Backend (Express)
    ↓ [Kafka Producer]
Kafka Topic: ecommerce
    ↓ [Consumer]
Spark Streaming → HDFS /brut/events/
    ↓ [Job quotidien]
Spark Batch → HDFS /indicators/
    ↓ [Parquet optimisé]
Dashboards & Visualisations
```

---

## 🛠️ Stack Technique

### **Big Data**
- ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white) - Traitement distribué
- ![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-66CCFF?style=flat-square) - Stockage distribué
- ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white) - Streaming événementiel
- ![Parquet](https://img.shields.io/badge/Parquet-FF6B6B?style=flat-square) - Format columaire

### **Backend (Équipe)**
- ![Node.js](https://img.shields.io/badge/Node.js-339933?style=flat-square&logo=node.js&logoColor=white) - Runtime
- ![Express.js](https://img.shields.io/badge/Express.js-000000?style=flat-square&logo=express&logoColor=white) - API REST
- ![MongoDB](https://img.shields.io/badge/MongoDB-13AA52?style=flat-square&logo=mongodb&logoColor=white) - Base données

### **Frontend (Équipe)**
- ![React](https://img.shields.io/badge/React-61DAFB?style=flat-square&logo=react&logoColor=black) - UI
- ![Vite](https://img.shields.io/badge/Vite-646CFF?style=flat-square&logo=vite&logoColor=white) - Build

### **DevOps**
- ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
- ![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

---

## 🚀 Lancement du Projet

### **Démarrer l'infrastructure Big Data + Backend**
```bash
docker compose up -d
```
→ Lance Hadoop, Spark, Kafka, MongoDB, Backend

### **Lancer le Frontend**
```bash
cd front
npm install
npm run dev
```
→ http://localhost:5173

### **Remplir la base de données**
```bash
cd back
npm start        # Serveur
npm run seed     # Ajouter données de test
```

---

## 💡 Décisions Architecturales Justifiées

### **1. Batch vs Streaming (j'ai choisi Batch)**

| Approche | Pros | Cons | Mon choix |
|----------|------|------|----------|
| **Streaming** | KPIs temps réel | Complexe, ressources | Futur |
| **Batch** ✅ | Simple, fiable, suffisant | Délai 24h | Actuel |

**Justification** : Les alertes rupture stock sont gérées en temps réel via le backend. Les KPIs peuvent attendre le batch quotidien.

### **2. Format de Stockage : Parquet**

✅ Compression 5-10× (économies de stockage)  
✅ Format columaire (requêtes analytiques rapides)  
✅ Standard Big Data (compatible Spark, Hive, etc.)

### **3. Partitionnement par Date**

```
/brut/events/2025-12-30/
/brut/events/2025-12-31/
```

Permet retrouver facilement toutes les commandes d'un jour → **facilite incidents, rejeu, analyses**.

---

## 📈 Résultats & Impact

**Données traitées** : 2574 événements  
**Conversion** : 39.6% (1844 vues → 730 achats)  
**Chiffre d'affaires** : 346 620€  
**Alertes générées** : 7 produits en rupture de stock  
**Fiabilité pipeline** : 100% (0 perte de données)

---

## 🎓 Compétences Démontrées

- ✅ Architecture Big Data **scalable** (brut → curated → indicators)
- ✅ Spark Streaming + Spark Batch (dual job pattern)
- ✅ HDFS organisation et gestion de données
- ✅ Kafka intégration (producer/consumer)
- ✅ Optimisation formats (Parquet, compression)
- ✅ KPIs métier (business analytics)
- ✅ Collabortation équipe (front/back/data)
- ✅ Docker & DevOps

---

## 👤 Auteur

**Hassan HOUSSEIN HOUMED**  
📚 Mastère 2 Big Data & Intelligence Artificielle - IPSSI Paris  
📧 hassan.houssein.houmed@gmail.com  
🐙 GitHub : https://github.com/HASSANHOUSSEINHOUMED

---

**Dernière mise à jour** : Décembre 2025
