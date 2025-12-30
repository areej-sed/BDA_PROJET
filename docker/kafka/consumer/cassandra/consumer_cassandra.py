from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime
import json
import time

# -----------------------------
# Connexion Cassandra
# -----------------------------
while True:
    try:
        cluster = Cluster(['cassandra1'])
        session = cluster.connect()
        print("Connecté à Cassandra")
        break
    except Exception as e:
        print("Cassandra non prête, retry dans 5s...")
        time.sleep(5)

# Création du keyspace si non existant
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS nyc_taxi
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
""")

session.execute("USE nyc_taxi")

# Création des tables si elles n'existent pas
session.execute("""
    CREATE TABLE IF NOT EXISTS taxi_trips_by_borough_hour (
        pickup_borough text,
        pickup_hour int,
        pickup_date date,
        trip_id bigint,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance float,
        fare_amount float,
        total_amount float,
        pickup_zone text,
        dropoff_zone text,
        pickup_latitude double,
        pickup_longitude double,
        dropoff_latitude double,
        dropoff_longitude double,
        PRIMARY KEY ((pickup_borough, pickup_hour, pickup_date), trip_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS passenger_distribution_by_borough (
        pickup_borough text,
        pickup_hour int,
        pickup_date date,
        passenger_count int,
        trips_count counter,
        PRIMARY KEY ((pickup_borough, pickup_hour, pickup_date), passenger_count)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS top_zones_by_amount_counter (
        pickup_borough text,
        pickup_zone text,
        total_amount_sum counter,
        PRIMARY KEY (pickup_borough, pickup_zone)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS anomalous_trips_by_borough (
        pickup_borough text,
        pickup_hour int,
        pickup_date date,
        trip_id bigint,
        anomaly_type text,
        trip_distance float,
        total_amount float,
        PRIMARY KEY ((pickup_borough, pickup_hour, pickup_date), trip_id)
    )
""")

print("Connecté à Cassandra et tables prêtes")

# -----------------------------
# Connexion Kafka
# -----------------------------
consumer = KafkaConsumer(
    'taxi.raw',
    bootstrap_servers=[
        'kafka1:9092',
        'kafka2:9092',
        'kafka3:9092'
    ],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='taxi-consumer-group'
)

print("Consumer Kafka démarré...")

# -----------------------------
# Consommation des messages
# -----------------------------
timeout_ms = 10000  # 10 seconds timeout to detect end of stream
while True:
    msg_pack = consumer.poll(timeout_ms=timeout_ms)
    if not msg_pack:
        # No messages received within timeout, assume all messages processed
        break
    for tp, messages in msg_pack.items():
        for msg in messages:
            trip = msg.value

            try:
                pickup_dt = datetime.fromisoformat(trip['pickup_datetime'])
                pickup_date = pickup_dt.date()
                pickup_hour = pickup_dt.hour
                pickup_borough = trip['pickup_borough']
                passenger_count = int(trip['passenger_count'])
                trip_distance = float(trip['trip_distance'])
                total_amount = float(trip['total_amount'])

                session.execute("""
                    INSERT INTO taxi_trips_by_borough_hour (
                        pickup_borough, pickup_hour, pickup_date, trip_id,
                        pickup_datetime, dropoff_datetime,
                        passenger_count, trip_distance,
                        fare_amount, total_amount,
                        pickup_zone, dropoff_zone,
                        pickup_latitude, pickup_longitude,
                        dropoff_latitude, dropoff_longitude
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    pickup_borough,
                    pickup_hour,
                    pickup_date,
                    trip['trip_id'],
                    pickup_dt,
                    datetime.fromisoformat(trip['dropoff_datetime']),
                    passenger_count,
                    trip_distance,
                    trip['fare_amount'],
                    total_amount,
                    trip['pickup_zone'],
                    trip['dropoff_zone'],
                    trip['pickup_latitude'],
                    trip['pickup_longitude'],
                    trip['dropoff_latitude'],
                    trip['dropoff_longitude']
                ))

                session.execute("""
                    UPDATE passenger_distribution_by_borough
                    SET trips_count = trips_count + 1
                    WHERE pickup_borough = %s
                      AND pickup_hour = %s
                      AND passenger_count = %s
                      AND pickup_date = %s
                """, (
                    pickup_borough,
                    pickup_hour,
                    passenger_count,
                    pickup_date
                ))

                session.execute("""
                    UPDATE top_zones_by_amount_counter
                    SET total_amount_sum = total_amount_sum + %s
                    WHERE pickup_borough = %s
                      AND pickup_zone = %s
                """, (
                    int(total_amount),
                    pickup_borough,
                    trip['pickup_zone']
                ))

                anomaly_type = None
                if trip_distance == 0:
                    anomaly_type = "DISTANCE_NULLE"
                elif trip_distance > 100:
                    anomaly_type = "DISTANCE_EXCESSIVE"

                if anomaly_type:
                    session.execute("""
                        INSERT INTO anomalous_trips_by_borough (
                            pickup_borough, pickup_hour, pickup_date,
                            trip_id, anomaly_type, trip_distance, total_amount
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        pickup_borough,
                        pickup_hour,
                        pickup_date,
                        trip['trip_id'],
                        anomaly_type,
                        trip_distance,
                        total_amount
                    ))

                print(f"Traité : {trip.get('trip_id')}")

            except Exception as e:
                print("Erreur traitement message :", e)
                time.sleep(1)

print("Consumer execution completed successfully.")
