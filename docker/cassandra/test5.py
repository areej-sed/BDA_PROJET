from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time

# Connexion au cluster
cluster = Cluster(['cassandra1', 'cassandra2', 'cassandra3'])
session = cluster.connect('nyc_taxi')

def test_read(cl):
    stmt = SimpleStatement(
        "SELECT * FROM taxi_trips_by_borough_date LIMIT 5",
        consistency_level=cl
    )
    start = time.time()
    try:
        rows = session.execute(stmt)
        elapsed = time.time() - start
        print(f"CL={cl}, SUCCESS, Time={elapsed:.3f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"CL={cl}, FAILURE, Time={elapsed:.3f}s, Error={e}")

consistency_levels = [ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.QUORUM]

while True:
    for cl in consistency_levels:
        test_read(cl)
    time.sleep(2)

print("Test terminé.")