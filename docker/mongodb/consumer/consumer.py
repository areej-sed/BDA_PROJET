"""
Enhanced MongoDB Consumer with Performance Tracking
Consumes from Kafka, inserts to MongoDB, and tracks ingestion metrics
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from datetime import datetime
import os


print("started")


# Configuration
KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9092', 'kafka3:9092']
KAFKA_TOPIC = 'taxi.raw'
MONGO_URI = 'mongodb://mongos:27017/'
DB_NAME = 'taxi_streaming'
COLLECTION_NAME = 'taxi_events'
BATCH_SIZE = 100
TARGET_EVENTS = int(os.getenv('TARGET_EVENTS', '500000'))
ENABLE_METRICS = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'

# Metrics storage
metrics = {
    'start_time': None,
    'total_events': 0,
    'total_batches': 0,
    'total_latency_ms': 0,
    'min_latency_ms': float('inf'),
    'max_latency_ms': 0,
    'latencies': [],
    'checkpoint_interval': 200000,  # Measure speed every 200K events
    'last_checkpoint_events': 0,
    'last_checkpoint_time': None,
    'checkpoint_speeds': []
}

print("=" * 50)
print("MongoDB Consumer Starting...")
if ENABLE_METRICS:
    print("📊 Performance Tracking: ENABLED")
print("=" * 50)

# Wait for services
time.sleep(60)

# Connect to MongoDB
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
collection = db[COLLECTION_NAME]
print("✅ Connected to MongoDB")

# Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
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
    "Staten Island": 5,
    "UNKNOWN":6
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
batch = []
metrics['start_time'] = time.time()
metrics['last_checkpoint_time'] = metrics['start_time']

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
        
        # Insert every BATCH_SIZE events
        if len(batch) >= BATCH_SIZE:
            batch_start = time.time()
            
            try:
                collection.insert_many(batch, ordered=False)
                
                # Track metrics
                if ENABLE_METRICS:
                    batch_latency = (time.time() - batch_start) * 1000  # ms
                    metrics['total_events'] += len(batch)
                    metrics['total_batches'] += 1
                    metrics['total_latency_ms'] += batch_latency
                    metrics['min_latency_ms'] = min(metrics['min_latency_ms'], batch_latency)
                    metrics['max_latency_ms'] = max(metrics['max_latency_ms'], batch_latency)
                    metrics['latencies'].append(batch_latency)
                    
                    # Calculate current stats
                    elapsed = time.time() - metrics['start_time']
                    throughput = metrics['total_events'] / elapsed if elapsed > 0 else 0
                    avg_latency = metrics['total_latency_ms'] / metrics['total_batches']
                    
                    # Check if we reached a 200K checkpoint
                    if metrics['total_events'] - metrics['last_checkpoint_events'] >= metrics['checkpoint_interval']:
                        checkpoint_time = time.time()
                        checkpoint_events = metrics['total_events'] - metrics['last_checkpoint_events']
                        checkpoint_duration = checkpoint_time - metrics['last_checkpoint_time']
                        checkpoint_speed = checkpoint_events / checkpoint_duration
                        
                        metrics['checkpoint_speeds'].append({
                            'events': metrics['total_events'],
                            'speed': checkpoint_speed,
                            'timestamp': datetime.now().isoformat()
                        })
                        
                        print("\n" + "=" * 70)
                        print(f"📊 CHECKPOINT: {metrics['total_events']:,} events")
                        print(f"   Speed (last {checkpoint_events:,} events): {checkpoint_speed:.2f} events/sec")
                        print(f"   Time for last {checkpoint_events:,} events: {checkpoint_duration:.2f}s")
                        print(f"   Overall average: {throughput:.2f} events/sec")
                        print("=" * 70 + "\n")
                        
                        metrics['last_checkpoint_events'] = metrics['total_events']
                        metrics['last_checkpoint_time'] = checkpoint_time
                    
                    # Print progress every 10 batches
                    elif metrics['total_batches'] % 10 == 0:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                              f"Events: {metrics['total_events']:,} | "
                              f"Throughput: {throughput:.1f} ev/s | "
                              f"Latency: {batch_latency:.1f}ms (avg: {avg_latency:.1f}ms)")
                else:
                    metrics['total_events'] += len(batch)
                    if metrics['total_events'] % 1000 == 0:
                        print(f"✅ Inserted {len(batch)} events | Total: {metrics['total_events']:,}")
                
                batch = []

                # Stop if target reached
                if metrics['total_events'] >= TARGET_EVENTS:
                    print(f"\n✅ Target of {TARGET_EVENTS:,} events reached!")
                    break
                
            except Exception as e:
                print(f"❌ Error: {e}")
                batch = []

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")
    
    # Insert remaining
    if batch:
        try:
            collection.insert_many(batch, ordered=False)
            metrics['total_events'] += len(batch)
            print(f"✅ Inserted final {len(batch)} events")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    # Print final metrics
    if ENABLE_METRICS and metrics['total_batches'] > 0:
        total_time = time.time() - metrics['start_time']
        avg_throughput = metrics['total_events'] / total_time
        avg_latency = metrics['total_latency_ms'] / metrics['total_batches']
        
        # Calculate percentiles
        sorted_latencies = sorted(metrics['latencies'])
        p50_idx = int(len(sorted_latencies) * 0.50)
        p95_idx = int(len(sorted_latencies) * 0.95)
        p99_idx = int(len(sorted_latencies) * 0.99)
        
        print("\n" + "=" * 60)
        print("PERFORMANCE SUMMARY")
        print("=" * 60)
        print(f"Total events: {metrics['total_events']:,}")
        print(f"Total time: {total_time:.2f}s")
        print(f"Average throughput: {avg_throughput:.2f} events/sec")
        print(f"\nLatency Statistics:")
        print(f"  Average: {avg_latency:.2f}ms")
        print(f"  Min: {metrics['min_latency_ms']:.2f}ms")
        print(f"  Max: {metrics['max_latency_ms']:.2f}ms")
        if len(sorted_latencies) > 0:
            print(f"  P50: {sorted_latencies[p50_idx]:.2f}ms")
            print(f"  P95: {sorted_latencies[p95_idx]:.2f}ms")
            print(f"  P99: {sorted_latencies[p99_idx]:.2f}ms")
        
        # Display checkpoint speeds
        if metrics['checkpoint_speeds']:
            print(f"\n📊 Ingestion Speed per 200K Events:")
            for i, checkpoint in enumerate(metrics['checkpoint_speeds'], 1):
                print(f"  Checkpoint {i} (at {checkpoint['events']:,} events): {checkpoint['speed']:.2f} events/sec")
        
        print("=" * 60)
        
        # Save metrics to file
        try:
            metrics_summary = {
                'timestamp': datetime.now().isoformat(),
                'total_events': metrics['total_events'],
                'total_time_seconds': total_time,
                'average_throughput_events_per_sec': avg_throughput,
                'latency_stats_ms': {
                    'average': avg_latency,
                    'min': metrics['min_latency_ms'],
                    'max': metrics['max_latency_ms'],
                    'p50': sorted_latencies[p50_idx] if sorted_latencies else 0,
                    'p95': sorted_latencies[p95_idx] if sorted_latencies else 0,
                    'p99': sorted_latencies[p99_idx] if sorted_latencies else 0
                },
                'checkpoint_speeds_per_200k_events': metrics['checkpoint_speeds']
            }
            
            with open('/metrics/ingestion_summary.json', 'w') as f:
                json.dump(metrics_summary, f, indent=2)
            print("📊 Metrics saved to /metrics/ingestion_summary.json")
        except Exception as e:
            print(f"⚠️ Could not save metrics: {e}")
    
    consumer.close()
    mongo.close()
    print(f"✅ Stopped. Total events: {metrics['total_events']:,}")