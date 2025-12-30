# Streaming Taxi avec Kafka, MongoDB et Cassandra

Projet du module **BDABD**

---

## 1. Description du projet

Ce projet a pour objectif de ....

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

## Partie 1 : Ingestion Kafka (streaming)

**Étape 1 : Inspecter le fichier Parquet**

```bash
cd data_preparation
python inspect_dataset.py
```

**Étape 2 : Conversion Parquet vers JSON streaming**

```bash
python prepare_dataset.py
```

**verefication**

```bash
python verifications.py
```

## Partie 2 : Mise en place du Cluster Kafka avec Docker

**Étape 1 :**

```bash
docker network create kafka-net
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

---
