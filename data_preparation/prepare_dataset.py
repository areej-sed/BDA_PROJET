import pandas as pd
import json
import uuid
from tqdm import tqdm

INPUT_FILE = "data/yellow_tripdata_2024_01.parquet"
OUTPUT_FILE = "data/nyc_taxi_events.json"

df = pd.read_parquet(INPUT_FILE)

zones = pd.read_csv("data/taxi_zone_lookup.csv")
zones.set_index("LocationID", inplace=True)


df = df.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount"
])

events = []

#pas de
#pickup_location.longitude
#pickup_location.latitude

for _, row in tqdm(df.iterrows(), total=len(df)):
    pickup_zone = zones.loc[row["PULocationID"]]

    event = {
        "trip_id": str(uuid.uuid4()),
        "pickup_datetime": row["tpep_pickup_datetime"].isoformat(),
        "pickup_location": {
            "borough_id": int(row["PULocationID"])
        },
        "dropoff_location": int(row["DOLocationID"]),
        "passenger_count": int(row["passenger_count"]),
        "trip_distance": float(row["trip_distance"]),
        "fare_amount": float(row["fare_amount"])
    }

    event["pickup_location"] = {
    "borough": pickup_zone["Borough"],
    "zone": pickup_zone["Zone"]
    }

    events.append(json.dumps(event))

# Écriture ligne par ligne
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for e in events:
        f.write(e + "\n")

print(f"Dataset prêt : {len(events)} événements")
