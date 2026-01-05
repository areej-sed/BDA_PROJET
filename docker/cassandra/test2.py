from cassandra.cluster import Cluster

# Connexion au cluster
cluster = Cluster(['cassandra1', 'cassandra2', 'cassandra3'])
session = cluster.connect()

# Créer le keyspace si pas déjà existant
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS nyc_taxi
    WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
""")

# Utiliser le keyspace
session.set_keyspace('nyc_taxi')

# -----------------------------
# Table 1 : partition_key = borough + hour
# -----------------------------
session.execute("""
    CREATE TABLE IF NOT EXISTS taxi_trips_borough_hour (
        pickup_borough text,
        pickup_hour int,
        trip_id bigint,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance double,
        fare_amount double,
        total_amount double,
        PRIMARY KEY ((pickup_borough, pickup_hour), trip_id)
    );
""")

# -----------------------------
# Table 2 : partition_key = pickup_bucket (fenêtre temporelle)
# -----------------------------
session.execute("""
    CREATE TABLE IF NOT EXISTS taxi_trips_bucket (
        pickup_bucket text,
        trip_id bigint,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance double,
        fare_amount double,
        total_amount double,
        PRIMARY KEY ((pickup_bucket), trip_id)
    );
""")

# -----------------------------
# Table 3 : partition_key = UUID aléatoire
# -----------------------------
session.execute("""
    CREATE TABLE IF NOT EXISTS taxi_trips_uuid (
        trip_id uuid,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        pickup_borough text,
        passenger_count int,
        trip_distance double,
        fare_amount double,
        total_amount double,
        PRIMARY KEY ((trip_id), pickup_datetime)
    );
""")

print("Tables créées avec succès")
cluster.shutdown()
