le json du test contient les dates de 1/1/2024 -> 6/1/2024
TRACING ON;
---------Revenu total par borough et par heure
SELECT pickup_zone, total_amount, trips_count
FROM top_zones_by_amount
WHERE pickup_borough = 'Manhattan'
AND pickup_date = '2024-01-06'
AND pickup_hour = 17;


---------Top zones par revenus (classement)
SELECT pickup_zone, total_amount
FROM top_zones_by_amount
WHERE pickup_borough = 'Brooklyn'
AND pickup_date = '2024-01-06'
AND pickup_hour = 9;


---------Volume de trajets par borough / heure
SELECT pickup_zone, trips_count
FROM top_zones_by_amount
WHERE pickup_borough = 'Queens'
AND pickup_date = '2024-01-04'
AND pickup_hour = 8;

---------Trajets d’un borough sur une heure donnée
SELECT *
FROM taxi_trips_by_borough_date
WHERE pickup_borough = 'Bronx'
AND pickup_date = '2024-01-02'
AND pickup_hour = 14;

------------Distribution pour une heure donnée
SELECT passenger_count, trips_count
FROM passenger_distribution
WHERE pickup_borough = 'Manhattan'
AND pickup_date = '2024-01-05'
AND pickup_hour = 10;


------------Détection des trajets anormaux
-----Trajets à distance nulle
SELECT *
FROM anomalous_trips
WHERE anomaly_type = 'DISTANCE_NULLE'
AND pickup_date = '2024-01-01';


SELECT *
FROM anomalous_trips
WHERE anomaly_type = 'VITESSE_EXCESSIVE'
AND pickup_date = '2024-01-01';

SELECT *
FROM anomalous_trips
WHERE anomaly_type = 'TOTAL_NEGATIF'
AND pickup_date = '2024-01-03';

SELECT *
FROM anomalous_trips
WHERE anomaly_type = 'DATES_INCOHERENTES'
AND pickup_date = '2024-01-04';
--------------------------------------

le teste de la table secondaire:
récupérer tous les trajets d'un borough, date et heure précis
SELECT *
FROM taxi_trips_by_borough_date
WHERE pickup_borough = 'Brooklyn'
AND pickup_date = '2024-01-06'
AND pickup_hour = 9;
-->32.804ms
---table secondaire:
CREATE TABLE taxi_trips_by_date_hour (
    pickup_date date,
    pickup_hour int,
    pickup_borough text,
    pickup_datetime timestamp,
    trip_id bigint,
    total_amount double,
    PRIMARY KEY (
        (pickup_date, pickup_hour),
        pickup_borough,
        pickup_datetime,
        trip_id
    )
)
WITH CLUSTERING ORDER BY (
    pickup_borough ASC,
    pickup_datetime DESC,
    trip_id ASC
);


SELECT *
FROM taxi_trips_by_date_hour
WHERE pickup_date = '2024-01-06'
AND pickup_hour = 9
AND pickup_borough = 'Brooklyn'
ALLOW FILTERING;
-->2.078ms
