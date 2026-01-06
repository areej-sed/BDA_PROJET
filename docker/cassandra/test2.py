import time
import json
import uuid
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# ==============================
# Connexion Cassandra 
# ==============================
while True:
    try:
        cluster = Cluster(
            ['cassandra1'],
            connect_timeout=30
        )
        session = cluster.connect()
        session.default_timeout = 60
        print("✅ Connecté à Cassandra")
        break
    except Exception:
        print("⏳ Cassandra non prête, retry dans 5s...")
        time.sleep(5)

# ==============================
# Keyspace
# ==============================
session.execute("USE nyc_taxi")

# ==============================
# Tables
# ==============================
session.execute("""
CREATE TABLE IF NOT EXISTS taxi_by_borough_hour (
    pickup_borough text,
    pickup_hour int,
    pickup_datetime timestamp,
    trip_id uuid,
    total_amount double,
    pickup_zone text,
    PRIMARY KEY ((pickup_borough, pickup_hour), pickup_datetime, trip_id)
) WITH CLUSTERING ORDER BY (pickup_datetime DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS taxi_by_time_bucket (
    time_bucket text,
    pickup_datetime timestamp,
    trip_id uuid,
    pickup_borough text,
    total_amount double,
    PRIMARY KEY (time_bucket, pickup_datetime, trip_id)
) WITH CLUSTERING ORDER BY (pickup_datetime DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS taxi_by_random_uuid (
    random_id uuid,
    trip_id uuid,
    pickup_datetime timestamp,
    pickup_borough text,
    total_amount double,
    PRIMARY KEY (random_id, trip_id)
);
""")

print(" Tables prêtes")

# ==============================
# Prepared Statements
# ==============================
insert_borough = session.prepare("""
INSERT INTO taxi_by_borough_hour (
    pickup_borough, pickup_hour, pickup_datetime,
    trip_id, total_amount, pickup_zone
) VALUES (?, ?, ?, ?, ?, ?)
""")

insert_bucket = session.prepare("""
INSERT INTO taxi_by_time_bucket (
    time_bucket, pickup_datetime, trip_id,
    pickup_borough, total_amount
) VALUES (?, ?, ?, ?, ?)
""")

insert_random = session.prepare("""
INSERT INTO taxi_by_random_uuid (
    random_id, trip_id, pickup_datetime,
    pickup_borough, total_amount
) VALUES (?, ?, ?, ?, ?)
""")

# ==============================
# Lecture JSON
# ==============================
with open("/data/data_500k.json", "r", encoding="utf-8") as f:
    trips = json.load(f)

print(f" {len(trips)} lignes à insérer")

# ==============================
# Insertion ASYNCHRONE (clé)
# ==============================
futures = []
MAX_IN_FLIGHT = 500   # limite de requêtes simultanées

for i, trip in enumerate(trips, start=1):
    pickup_datetime = datetime.fromisoformat(trip["pickup_datetime"])
    pickup_hour = pickup_datetime.hour
    pickup_borough = trip["pickup_borough"]
    pickup_zone = trip.get("pickup_zone", "UNKNOWN")
    total_amount = float(trip["total_amount"])

    trip_id = uuid.uuid4()
    random_id = uuid.uuid4()
    time_bucket = pickup_datetime.strftime("%Y-%m-%d-%H")

    futures.append(
        session.execute_async(insert_borough, (
            pickup_borough, pickup_hour, pickup_datetime,
            trip_id, total_amount, pickup_zone
        ))
    )

    futures.append(
        session.execute_async(insert_bucket, (
            time_bucket, pickup_datetime, trip_id,
            pickup_borough, total_amount
        ))
    )

    futures.append(
        session.execute_async(insert_random, (
            random_id, trip_id, pickup_datetime,
            pickup_borough, total_amount
        ))
    )

    # Backpressure
    if len(futures) >= MAX_IN_FLIGHT:
        for f in futures:
            f.result()
        futures.clear()

    if i % 10000 == 0:
        print(f"→ {i} lignes traitées")

# Attente finale
for f in futures:
    f.result()

print(" Insertion terminée")
cluster.shutdown()

