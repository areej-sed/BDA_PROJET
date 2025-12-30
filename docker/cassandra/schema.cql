CREATE KEYSPACE IF NOT EXISTS nyc_taxi
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
};

USE nyc_taxi;

-- 1. Table principale pour les trajets par borough et heure
CREATE TABLE taxi_trips_by_borough_hour (
    pickup_borough text,
    pickup_hour int,            
    pickup_date date,          
    trip_id bigint,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count float,
    trip_distance float,
    fare_amount float,
    total_amount float,
    pickup_zone text,
    dropoff_zone text,
    pickup_latitude double,
    pickup_longitude double,
    dropoff_latitude double,
    dropoff_longitude double,
    PRIMARY KEY ((pickup_borough, pickup_hour), pickup_date, trip_id)
) WITH CLUSTERING ORDER BY (pickup_date ASC);

-- 2. Table pour top zones par montant 
CREATE TABLE top_zones_by_amount_counter (
    pickup_borough text,
    pickup_zone text,
    total_amount_sum counter,
    PRIMARY KEY (pickup_borough, pickup_zone)
);

-- 3. Table pour distribution du nombre de passagers
CREATE TABLE passenger_distribution_by_borough (
    pickup_borough text,
    pickup_hour int,            
    pickup_date date,          
    passenger_count int,
    trips_count counter,
    PRIMARY KEY ((pickup_borough, pickup_hour, passenger_count), pickup_date)
) WITH CLUSTERING ORDER BY (pickup_date ASC);

-- 4. Table pour les trajets anormaux
CREATE TABLE anomalous_trips_by_borough (
    pickup_borough text,
    pickup_hour int,            
    pickup_date date,           
    trip_id bigint,
    anomaly_type text,
    trip_distance float,
    total_amount float,
    PRIMARY KEY ((pickup_borough, pickup_hour), pickup_date, trip_id)
) WITH CLUSTERING ORDER BY (pickup_date ASC);
