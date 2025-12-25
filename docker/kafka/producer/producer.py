from kafka import KafkaProducer
import json
import time

def send_event(event):
    key = str(event.get("pickup_borough", "UNKNOWN"))

    producer.send(
        "taxi.raw",
        key=key,
        value=event
    )

    print(f"Envoyé : {event.get('trip_id')} | key={key}")
    time.sleep(0.2)

producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092'],
    api_version=(3, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

with open("data/final_data.json", "r") as f:
    first_char = f.read(1)
    f.seek(0)

    if first_char == "[":
        events = json.load(f)
        for event in events:
            send_event(event)
    

producer.flush()
producer.close()
