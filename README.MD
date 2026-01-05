# Streaming Taxi avec Kafka, MongoDB et Cassandra

Projet du module **BDABD**

Préparé par : 
SEDDIKI Aridj
BOUREKIA Nihad
KALACHE Rachid Abdelghafour
---

Lien du rapport de projet : https://shorturl.at/sqPn4

---

## 1. Description du projet

Ce projet vise à concevoir et tester un pipeline Big Data temps réel pour les flux NYC Taxi. Kafka est utilisé pour le streaming, tandis que MongoDB et Cassandra servent au stockage et à l’analyse des données. L’objectif est de maîtriser le traitement en streaming, la modélisation NoSQL, et d’évaluer les performances et la scalabilité des clusters, le tout orchestré via Docker.
---

## 1.1 Structure du projet

Voici l'arborescence des fichiers importants et leur description :

```
BDA_PROJET/
├── data/                       # Dossier de stockage des données
│   ├── input/                  # Données brutes (Parquet, Shapefiles)
│   └── output/                 # Données traitées (JSON)
│
├── data_preparation/           # Scripts de préparation des données
│   └── data_preprocessing.ipynb # Notebook principal pour le nettoyage et la conversion des données
│
├── docker/                     # Configuration des conteneurs Docker
│   ├── cassandra/              # Environnement Cassandra
│   │   ├── docker-compose_cassandra.yml
│   │   └── schema.cql          # Schéma de la base de données
│   │
│   ├── kafka/                  # Environnement Kafka
│   │   ├── consumer/           # Consumers Kafka
│   │   │   └── cassandra/      # Consumer vers Cassandra
│   │   │       ├── consumer_cassandra.py
│   │   │       ├── Dockerfile
│   │   │       └── requirements.txt
│   │   ├── producer/           # Producer Kafka
│   │   │   ├── producer.py
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   └── docker-compose.yml  # Cluster Kafka
│   │
│   └── mongodb/                # Environnement MongoDB
│       ├── consumer/           # Consumer vers MongoDB
│       │   ├── consumer.py
│       │   ├── Dockerfile
│       │   └── requirements.txt
│       ├── docker-compose.yml  # Cluster MongoDB
│       ├── init-mongo.sh
│       └── mongo-init.js
│
└── README.md                   # Documentation du projet
```

---

## 2. Prérequis

Avant d’exécuter l’application, il est nécessaire d’avoir installé :

- **Python 3.8 ou supérieur**
- **pip**

- **Docker Desktop**

### Bibliothèques Python requises

- pandas
- pyarrow
- numpy
- python-dateutil
- tqdm
- ...

---

## 3. Installation et exécution

### Étape 1 : Cloner ou télécharger le projet

```bash
git clone https://github.com/areej-sed/taxi_streaming_project
cd taxi_streaming_project
```

**Étape 2 : Créer et activer un environnement virtuel (optionnel mais recommandé)**

```bash
python -m venv venv
```

\*Activation

```bash
venv\Scripts\activate
```

**Étape 3 : Installer les dépendances**

```bash
pip install pandas pyarrow numpy python-dateutil tqdm
```

## 4.

## Partie 1 : Préparation des données

Toutes les étapes de prétraitement des données se trouvent dans le notebook Jupyter :
**`data_preparation/data_preprocessing.ipynb`**

Les principales étapes réalisées dans ce notebook sont :
- Chargement des données brutes (Parquet) et des zones de taxi (Shapefile).
- Calcul des latitudes et longitudes (centroïdes) à partir des géométries des zones.
- Enrichissement des données : jointure pour ajouter les coordonnées et les noms de quartiers (Boroughs/Zones) aux points de prise en charge et de dépose.
- Sélection des colonnes pertinentes et renommage pour uniformisation.
- Ajout d'un identifiant unique `trip_id`.
- Export des données traitées en fichier JSON (`final_data.json`) pour l'ingestion.


## Partie 2 : Mise en place du Cluster Kafka avec Docker

**Étape 1 : Construction et Démarrage du Cluster (sans Producer)**
```bash
cd docker/kafka
docker compose build
docker compose up -d zookeeper kafka1 kafka2 kafka3
```

**Étape 2 : Création du topic Kafka taxi.raw**

Création du topic :
```bash
docker exec -it kafka1 kafka-topics --create --topic taxi.raw --bootstrap-server kafka1:9092 --partitions 6 --replication-factor 2
```

Vérifier les offsets (optionnel) :
```bash
docker exec -it kafka1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka1:9092 --topic taxi.raw
```

**Étape 3 : Lancer le Producer**
```bash
docker compose up producer
```

**Arrêt du cluster (avec suppression des volumes)**
```bash
docker compose down -v
```

---

## Partie 3 : Cluster MongoDB (Sharding) et Consumer

**Étape 1 : Démarrage du cluster**
```bash
cd docker/mongodb
docker compose build
docker compose up -d configsvr shard1 shard2 shard3 mongos mongo-init
```

**Étape 2 : Vérification de l'initialisation**
```bash
docker logs -f mongo-init
```

**Étape 3 : Démarrage du Consumer**
```bash
docker compose up consumer
```

**Arrêt du cluster (avec suppression des volumes)**
```bash
docker compose down -v
```



_Sortie attendue :_
```
✅ Connected to MongoDB
✅ Connected to Kafka
🔄 Listening for messages...
✅ Inserted 100 events | Total: 100
...
```

**Vérification finale : Comptage des documents**

```bash
docker exec -it mongos mongosh --eval "use taxi_streaming; db.taxi_events.countDocuments()"
```

**Vérification de la distribution des données (Sharding)**

```bash
docker exec -it mongos mongosh --eval "use taxi_streaming; db.taxi_events.getShardDistribution()"
```

_Example de Sortie attendue :_
```
Shard shard1rs at shard1rs/mongo-shard1:27018
{
  data: '1.32MiB',
  docs: 2624,
  chunks: 2,
  'estimated data per chunk': '676KiB',
  'estimated docs per chunk': 1312
}
---
Shard shard3rs at shard3rs/mongo-shard3:27018
{
  data: '32KiB',
  docs: 73,
  chunks: 2,
  'estimated data per chunk': '16KiB',
  'estimated docs per chunk': 36
}
---
Shard shard2rs at shard2rs/mongo-shard2:27018
{
  data: '7.8MiB',
  docs: 15203,
  chunks: 2,
  'estimated data per chunk': '3.9MiB',
  'estimated docs per chunk': 7601
}
---
Totals
{
  data: '9.15MiB',
  docs: 17900,
  chunks: 6,
  'Shard shard1rs': [
    '14.43 % data',
    '14.65 % docs in cluster',
    '528B avg obj size on shard'
  ],
  'Shard shard3rs': [
    '0.34 % data',
    '0.4 % docs in cluster',
    '449B avg obj size on shard'
  ],
  'Shard shard2rs': [
    '85.22 % data',
    '84.93 % docs in cluster',
    '538B avg obj size on shard'
  ]
}
```
## Partie 4 : Mise en place du Cluster Cassandra avec Docker

**Étape 1 : Lancer le cluster Cassandra**

```bash
cd docker/cassandra
docker-compose -f docker-compose_cassandra.yml up -d
```

**Vérification**

```bash
docker ps
```

**Étape 2 : Lancer le consumer pour creer et remplir les tables**

```bash
cd docker/kafka
docker compose up -d consumer_cassandra
```

