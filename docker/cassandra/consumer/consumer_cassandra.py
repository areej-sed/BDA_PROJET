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
        session = cluster.connect('nyc_taxi')
        print("Connecté à Cassandra")
        break
    except Exception as e:
        print("Cassandra non prête, retry dans 5s...")
        time.sleep(5)

# -----------------------------
# Connexion Kafka
# -----------------------------
while True:
    try:
        consumer = KafkaConsumer(
            'taxi.raw',
            bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='taxi-consumer-group'
        )
        print("Consumer Kafka démarré...")
        break
    except Exception as e:
        print("Kafka non prêt, retry dans 5s...")
        time.sleep(5)



# -----------------------------
# Consommation des messages
# -----------------------------
timeout_ms = 60000  # 1 minute
processed = 0
start_time = time.time()
latencies = []

while True:
    msg_pack = consumer.poll(timeout_ms=timeout_ms)
    if not msg_pack:
        break  # Aucun message reçu → fin du flux

    for tp, messages in msg_pack.items():
        for msg in messages:
            trip = msg.value

            try:
                pickup_dt = datetime.fromisoformat(trip['pickup_datetime'])
                pickup_date = pickup_dt.date()
                dropoff_dt = datetime.fromisoformat(trip['dropoff_datetime'])
                dropoff_date = dropoff_dt.date()
                pickup_hour = pickup_dt.hour
                pickup_borough = trip['pickup_borough']
                passenger_count = int(trip['passenger_count'])
                trip_distance = float(trip['trip_distance'])
                fare_amount = float(trip['fare_amount'])
                total_amount = float(trip['total_amount'])
                duration_seconds = (dropoff_dt - pickup_dt).total_seconds()
                duration_hours = duration_seconds / 3600 if duration_seconds > 0 else 0
                speed = trip_distance / duration_hours if duration_hours > 0 else 0
                # -----------------------------
                # INSERT dans taxi_trips_by_borough_date
                # -----------------------------
                session.execute("""
                    INSERT INTO taxi_trips_by_borough_date (
                        pickup_borough, pickup_date, pickup_hour, pickup_datetime,
                        trip_id, dropoff_datetime, passenger_count, trip_distance,
                        fare_amount, total_amount, pickup_zone, dropoff_zone,
                        pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    pickup_borough, pickup_date, pickup_hour, pickup_dt,
                    trip['trip_id'], datetime.fromisoformat(trip['dropoff_datetime']),
                    passenger_count, trip_distance, fare_amount, total_amount,
                    trip['pickup_zone'], trip['dropoff_zone'],
                    float(trip['pickup_latitude']), float(trip['pickup_longitude']),
                    float(trip['dropoff_latitude']), float(trip['dropoff_longitude'])
                ))

                # -----------------------------
                # UPDATE counters top_zones_by_amount
                # -----------------------------
                session.execute("""
                    UPDATE top_zones_by_amount
                    SET total_amount = total_amount + %s,
                        trips_count = trips_count + 1
                    WHERE pickup_borough = %s
                      AND pickup_date = %s
                      AND pickup_hour = %s
                      AND pickup_zone = %s
                """, (
                    int(total_amount),
                    pickup_borough,
                    pickup_date,
                    pickup_hour,
                    trip['pickup_zone']
                ))

                # -----------------------------
                # UPDATE counters passenger_distribution
                # -----------------------------
                session.execute("""
                    UPDATE passenger_distribution
                    SET trips_count = trips_count + 1
                    WHERE pickup_borough = %s
                      AND pickup_date = %s
                      AND pickup_hour = %s
                      AND passenger_count = %s
                """, (
                    pickup_borough,
                    pickup_date,
                    pickup_hour,
                    passenger_count
                ))

                # -----------------------------
                # INSERT anomalous_trips si nécessaire
                # -----------------------------
                anomaly_type = None
                if trip_distance == 0:
                    anomaly_type = "DISTANCE_NULLE"
                elif speed > 100:
                    anomaly_type = "VITESSE_EXCESSIVE"
                elif fare_amount < 0:
                    anomaly_type = "FARE_NEGATIF"
                elif total_amount < 0:
                    anomaly_type = "TOTAL_NEGATIF"
                elif pickup_date > dropoff_date:
                    anomaly_type = "DATES_INCOHERENTES"


                if anomaly_type:
                    session.execute("""
                        INSERT INTO anomalous_trips (
                            anomaly_type, pickup_date, pickup_borough, pickup_datetime,
                            trip_id, trip_distance, total_amount
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        anomaly_type,
                        pickup_date,
                        pickup_borough,
                        pickup_dt,
                        trip['trip_id'],
                        trip_distance,
                        total_amount
                    ))

                # -----------------------------
                # Mesure de la latence end-to-end
                # -----------------------------
                consume_time = time.time()
                produce_time = trip.get("kafka_produce_time")
                if produce_time:
                    latency_ms = (consume_time - produce_time) * 1000
                    latencies.append(latency_ms)

                # -----------------------------
                # Affichage et commit tous les 1000 events
                # -----------------------------
                processed += 1
                if processed % 1000 == 0:
                    consumer.commit()
                    elapsed = time.time() - start_time
                    print(f"{processed} events traités {elapsed:.2f}s | Débit ≈ {processed/elapsed:.2f} events/s")

            except Exception as e:
                print("Erreur traitement message :", e)
                time.sleep(0.5)

# -----------------------------
# Statistiques de latence
# -----------------------------
if latencies:
    avg_latency = sum(latencies) / len(latencies)
    avg_latency = avg_latency / 1000
    max_latency = max(latencies)
    max_latency = max_latency / 1000
    min_latency = min(latencies)
    min_latency = min_latency / 1000
    print("\n" + "="*50)
    print("LATENCE END-TO-END (Kafka → Cassandra)")
    print("="*50)
    print(f"Events mesurés : {len(latencies)}")
    print(f"Latence moyenne : {avg_latency:.2f} s")
    print(f"Latence min     : {min_latency:.2f} s")
    print(f"Latence max     : {max_latency:.2f} s")
    print("="*50)

print("Consumer execution completed successfully.")
consumer.close()