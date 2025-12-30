from kafka import KafkaProducer
import json
import time
import psutil
import os
from datetime import datetime

# Initialize monitoring
process = psutil.Process(os.getpid())
start_time = time.time()
initial_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
event_counter = 0

def send_event(event):
    global event_counter
    event_counter += 1
    key = str(event.get("pickup_borough", "UNKNOWN"))

    producer.send(
        "taxi.raw",
        key=key,
        value=event
    )

    # Print every 1000 events
    if event_counter % 1000 == 0:
        print(f"Envoyé : {event.get('trip_id')} | key={key} | Total events: {event_counter}")
    # time.sleep(0.0001)

producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092'],
    api_version=(3, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)
#  j'ai modifier le fichier pour lire le fichier final_data_updated.json pour tester le producer avec les données modifiées
with open("data/final_data_updated.json", "r") as f:   
    first_char = f.read(1)
    f.seek(0)

    if first_char == "[":
        events = json.load(f)
        for event in events:
            send_event(event)
    

producer.flush()
producer.close()

# Calculate and display metrics
end_time = time.time()
execution_time = end_time - start_time
final_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
memory_used = final_memory - initial_memory
cpu_percent = process.cpu_percent(interval=1)

print("\n" + "="*50)
print("PERFORMANCE METRICS")
print("="*50)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Initial Memory: {initial_memory:.2f} MB")
print(f"Final Memory: {final_memory:.2f} MB")
print(f"Memory Used: {memory_used:.2f} MB")
print(f"CPU Usage: {cpu_percent:.2f}%")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*50)
