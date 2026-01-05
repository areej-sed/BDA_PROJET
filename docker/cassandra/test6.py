from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import random
import time

cluster = Cluster(['cassandra1', 'cassandra2', 'cassandra3'])
session = cluster.connect('nyc_taxi')

def insert_trip(trip_id, cl):
    stmt = SimpleStatement(
        "INSERT INTO taxi_trips_by_borough_date (pickup_borough, pickup_date, pickup_hour, pickup_datetime, trip_id, trip_distance, total_amount) "
        "VALUES ('Manhattan', '2026-01-05', 12, '2026-01-05T12:00:00', %s, 10, 20)",
        consistency_level=cl
    )
    try:
        session.execute(stmt, (trip_id,))
        start = time.time()
        session.execute(stmt, (trip_id,))
        elapsed = time.time() - start
        print(f"INSERT trip {trip_id} SUCCESS, CL={cl}, Time={elapsed:.3f}s")
    except Exception as e:
        print(f"INSERT trip {trip_id} FAIL, CL={cl}, Error={e}")

def read_trip(trip_id, cl):
    stmt = SimpleStatement(
        "SELECT * FROM taxi_trips_by_borough_date WHERE pickup_borough='Manhattan' AND pickup_date='2026-01-05' AND pickup_hour=12 AND trip_id=%s",
        consistency_level=cl
    )
    start = time.time()
    rows = session.execute(stmt, (trip_id,))
    elapsed = time.time() - start
    print(f"READ trip {trip_id}, rows={len(list(rows))}, CL={cl}, Time={elapsed:.3f}s")
   
    return list(rows)

consistency_levels = [ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.QUORUM]

# Test avec pannes simulées
for i in range(1, 6):
    cl = random.choice(consistency_levels)
    insert_trip(i, cl)
    rows = read_trip(i, cl)
    print(f"Trip {i} read: {len(rows)} rows, CL={cl}")
    time.sleep(1)
print("Test terminé.")