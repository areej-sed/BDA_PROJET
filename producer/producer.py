from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

topic = "taxi.raw"

with open("data/nyc_taxi_events.json", "r") as f:
    for line in f:
        event = json.loads(line)

        # clé de partitionnement
        key = event["pickup_borough"]

        producer.send(
            topic,
            key=key,
            value=event
        )

        print(f"Envoyé : {event['trip_id']}")
        time.sleep(0.5)  # simulation temps réel

producer.flush()
producer.close()
