CREATE KEYSPACE IF NOT EXISTS nyc_taxi
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
};

USE nyc_taxi;

-- 1. Table principale pour les trajets par borough et heure
CREATE TABLE taxi_trips_by_borough_date (
        pickup_borough text,
        pickup_date date,
        pickup_hour int,
        pickup_datetime timestamp,
        trip_id bigint,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance double,
        fare_amount double,
        total_amount double,
        pickup_zone text,
        dropoff_zone text,
        pickup_latitude double,
        pickup_longitude double,
        dropoff_latitude double,
        dropoff_longitude double,
        PRIMARY KEY (
                (pickup_borough, pickup_date),
                pickup_hour,
                pickup_datetime,
                trip_id
        )
    )
    WITH CLUSTERING ORDER BY (
    pickup_hour ASC,
    pickup_datetime DESC,
    trip_id ASC
    );

-- 2. Table pour top zones par montant 
CREATE TABLE top_zones_by_amount (
        pickup_borough text,
        pickup_date date,
        pickup_hour int,
        pickup_zone text,
        total_amount counter,
        trips_count counter,
        PRIMARY KEY (
            (pickup_borough, pickup_date, pickup_hour),
            pickup_zone
        )
    );

-- 3. Table pour distribution du nombre de passagers
 CREATE TABLE passenger_distribution (
        pickup_borough text,
        pickup_date date,
        pickup_hour int,
        passenger_count int,
        trips_count counter,
        PRIMARY KEY (
            (pickup_borough, pickup_date, pickup_hour),
            passenger_count
        )
    );

-- 4. Table pour les trajets anormaux
 CREATE TABLE anomalous_trips (
        anomaly_type text,
        pickup_date date,
        pickup_borough text,
        pickup_datetime timestamp,
        trip_id bigint,
        trip_distance double,
        total_amount double,
        PRIMARY KEY (
            (anomaly_type, pickup_date),
            pickup_datetime,
            trip_id
        )
    );
