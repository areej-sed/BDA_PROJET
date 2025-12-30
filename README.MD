# Streaming Taxi avec Kafka, MongoDB et Cassandra

Projet du module **BDABD**

---

## 1. Description du projet

Ce projet a pour objectif de ....

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
│   ├── kafka/                  # Environnement Kafka
│   │   ├── docker-compose.yml  # Déploiement du cluster Kafka (Zookeeper, Brokers, Control Center)
│   │   └── producer/           # Producer Kafka (Python)
│   │       ├── producer.py     # Script d'envoi des données vers Kafka
│   │       ├── Dockerfile      # Image Docker du producer
│   │       └── requirements.txt # Dépendances Python du producer
│   │
│   └── mongodb/                # Environnement MongoDB
│       ├── consumer/           # Consumer Kafka vers MongoDB (Python)
│       │   ├── consumer.py     # Script d'ingestion des données
│       │   ├── Dockerfile      # Image Docker du consumer
│       │   └── requirements.txt # Dépendances Python du consumer
│       ├── docker-compose.yml  # Déploiement du cluster MongoDB (Config, Shards, Router, Consumer)
│       └── init-mongo.sh       # Script d'initialisation
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

**Étape 1 : Inspecter le fichier Parquet**

```bash
cd docker/kafka
docker compose up -d
```

**verefication**

```bash
docker ps
```

**verefication des logs**

```bash
docker logs kafka1
```

**Étape 2 : Création du topic Kafka taxi.raw**

Création du topic :
```bash
docker exec -it kafka1 kafka-topics --create --topic taxi.raw --bootstrap-server kafka1:9092 --partitions 6 --replication-factor 2
```

Vérifier si le topic existe :
```bash
docker exec -it kafka1 kafka-topics --list --bootstrap-server kafka1:9092
```

Supprimer le topic (si nécessaire pour réinitialiser les données) :
```bash
docker exec -it kafka1 kafka-topics --delete --topic taxi.raw --bootstrap-server kafka1:9092
```

**Étape 3 : Lancer le Producer**

```bash
docker compose up producer
```

---

## Partie 3 : Cluster MongoDB (Sharding) et Consumer

**Étape 1 : Démarrage du cluster MongoDB**

```bash
cd docker/mongodb
docker-compose up -d
```

**Étape 2 : Initialisation du Config Server**

```bash
docker exec -it mongo-config mongosh --port 27019 --eval "rs.initiate({_id:'configrs',configsvr:true,members:[{_id:0,host:'mongo-config:27019'}]})"
```

_Attendre 5 secondes..._

```powershell
Start-Sleep -Seconds 5
# ou "sleep 5" sur Linux/Mac
```

**Étape 3 : Initialisation des Shards**

```bash
docker exec -it mongo-shard1 mongosh --port 27018 --eval "rs.initiate({_id:'shard1rs',members:[{_id:0,host:'mongo-shard1:27018'}]})"
docker exec -it mongo-shard2 mongosh --port 27018 --eval "rs.initiate({_id:'shard2rs',members:[{_id:0,host:'mongo-shard2:27018'}]})"
docker exec -it mongo-shard3 mongosh --port 27018 --eval "rs.initiate({_id:'shard3rs',members:[{_id:0,host:'mongo-shard3:27018'}]})"
```

_Attendre 15 secondes..._

```powershell
Start-Sleep -Seconds 15
# ou "sleep 15" sur Linux/Mac
```

**Étape 4 : Ajout des Shards au Router**

```bash
docker exec -it mongos mongosh --eval "sh.addShard('shard1rs/mongo-shard1:27018')"
docker exec -it mongos mongosh --eval "sh.addShard('shard2rs/mongo-shard2:27018')"
docker exec -it mongos mongosh --eval "sh.addShard('shard3rs/mongo-shard3:27018')"
```

**Étape 5 : Activation du Sharding**

```bash
docker exec -it mongos mongosh --eval "sh.enableSharding('taxi_streaming')"
```

**Étape 6 : Création de la collection et de la clé de sharding**

```bash
docker exec -it mongos mongosh --eval "use taxi_streaming; db.createCollection('taxi_events'); db.taxi_events.createIndex({trip_id:1},{unique:true}); db.taxi_events.createIndex({pickup_borough_id:1}); sh.shardCollection('taxi_streaming.taxi_events',{pickup_borough_id:1})"
```

**Vérification du statut du Sharding**

```bash
docker exec -it mongos mongosh --eval "sh.status()"
```

**Étape 7 : Démarrage du Consumer**

```bash
docker restart mongo-consumer
docker logs -f mongo-consumer
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
