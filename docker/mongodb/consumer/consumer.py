from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from datetime import datetime

print("=" * 50)
print("MongoDB Consumer Starting...")
print("=" * 50)

# Wait for services
time.sleep(15)

# Connect to MongoDB
mongo = MongoClient('mongodb://mongos:27017/')
db = mongo['taxi_streaming']
collection = db['taxi_events']
print("✅ Connected to MongoDB")

# Connect to Kafka
consumer = KafkaConsumer(
    'taxi.raw',
    bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
    group_id='mongodb-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
print("✅ Connected to Kafka")
print("🔄 Listening for messages...\n")

# Borough mapping
BOROUGH_MAP = {
    "Manhattan": 1,
    "Brooklyn": 2,
    "Queens": 3,
    "Bronx": 4,
    "Staten Island": 5
}

# Helper function
def calculate_duration(pickup, dropoff):
    try:
        p = datetime.fromisoformat(pickup.replace('Z', ''))
        d = datetime.fromisoformat(dropoff.replace('Z', ''))
        return round((d - p).total_seconds() / 60, 2)
    except:
        return 0

# Process messages
count = 0
batch = []

try:
    for message in consumer:
        event = message.value
        
        # Add borough_id
        borough = event.get('pickup_borough', 'Unknown')
        event['pickup_borough_id'] = BOROUGH_MAP.get(borough, 6)
        
        # Calculate duration
        duration = calculate_duration(
            event.get('pickup_datetime', ''),
            event.get('dropoff_datetime', '')
        )
        
        # Calculate speed
        distance = event.get('trip_distance', 0)
        speed = round(distance / (duration / 60), 2) if duration > 0 else 0
        
        # Add calculated fields
        event['duration_minutes'] = duration
        event['speed_mph'] = speed
        
        # Add to batch
        batch.append(event)
        count += 1
        
        # Insert every 100 events
        if len(batch) >= 100:
            try:
                collection.insert_many(batch, ordered=False)
                print(f"✅ Inserted {len(batch)} events | Total: {count}")
                batch = []
            except Exception as e:
                print(f"❌ Error: {e}")
                batch = []

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")
    
    # Insert remaining
    if batch:
        try:
            collection.insert_many(batch, ordered=False)
            print(f"✅ Inserted final {len(batch)} events")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    consumer.close()
    mongo.close()
    print(f"✅ Stopped. Total events: {count}")