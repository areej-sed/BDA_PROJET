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


    if event_counter % 1000 == 0:
        print(f"Envoyé : {event.get('trip_id')} | key={key} | Total events: {event_counter}")
    # time.sleep(0.0001)


producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092'],
    api_version=(3, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)


print("KAFKA PRODUCER - STREAMING TAXI DATA ======================")


with open("data/final_data.json", "r") as f:

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
<<<<<<< HEAD


# from kafka import KafkaProducer
# import json
# import time
# import psutil
# import os
# from datetime import datetime
# from collections import defaultdict

# # Initialize monitoring
# process = psutil.Process(os.getpid())
# start_time = time.time()
# initial_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
# event_counter = 0
# error_counter = 0
# partition_stats = defaultdict(int)

# def send_event(event):
#     global event_counter, error_counter
#     event_counter += 1
    
#     # Get and normalize borough
#     borough = event.get("pickup_borough", "UNKNOWN")
#     key = str(borough).strip().upper() if borough else "UNKNOWN"
    
#     try:
#         future = producer.send(
#             "taxi.raw",
#             key=key,
#             value=event
#         )
        
#         # Get partition info (non-blocking for performance)
#         if event_counter % 1000 == 0:
#             record_metadata = future.get(timeout=10)
#             partition = record_metadata.partition
#             print(f"✅ Envoyé {event_counter} événements | "
#                   f"Trip: {event.get('trip_id')} | Borough: {key} | "
#                   f"Partition: {partition}")
        
#         # Track partition distribution for final stats
#         record_metadata = future.get(timeout=10)
#         partition_stats[f"{key}→P{record_metadata.partition}"] += 1
            
#     except Exception as e:
#         error_counter += 1
#         print(f"❌ Erreur pour trip {event.get('trip_id')}: {e}")

# # Producer configuration with improved reliability
# producer = KafkaProducer(
#     bootstrap_servers=['kafka1:9092'],
#     api_version=(3, 5, 0),
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     key_serializer=lambda k: k.encode('utf-8'),
#     acks='all',  # Wait for all replicas (more reliable)
#     retries=3,   # Retry on failure
#     max_in_flight_requests_per_connection=5,
#     compression_type='gzip'  # Compress data for better network performance
# )

# print("="*50)
# print("KAFKA PRODUCER - STREAMING TAXI DATA")
# print("="*50)
# print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# print(f"Bootstrap Server: kafka1:9092")
# print(f"Topic: taxi.raw")
# print(f"Partitioning: By pickup_borough")
# print("="*50 + "\n")

# try:
#     with open("data/final_data.json", "r") as f:
#         first_char = f.read(1)
#         f.seek(0)

#         if first_char == "[":
#             events = json.load(f)
#             total_events = len(events)
#             print(f"📂 Loaded {total_events} events from file\n")
            
#             for event in events:
#                 send_event(event)
#         else:
#             print("⚠️ File format not recognized (expected JSON array)")
    
#     # Ensure all messages are sent
#     print("\n🔄 Flushing remaining messages...")
#     producer.flush()
#     print("✅ All messages flushed successfully")
    
# except FileNotFoundError:
#     print("❌ Error: data/final_data.json not found")
# except json.JSONDecodeError as e:
#     print(f"❌ JSON parsing error: {e}")
# except Exception as e:
#     print(f"❌ Unexpected error: {e}")
# finally:
#     producer.close()
#     print("🔒 Producer closed\n")

# # Calculate and display metrics
# end_time = time.time()
# execution_time = end_time - start_time
# final_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
# memory_used = final_memory - initial_memory
# cpu_percent = process.cpu_percent(interval=1)

# # Calculate throughput
# events_per_second = event_counter / execution_time if execution_time > 0 else 0
# success_rate = ((event_counter - error_counter) / event_counter * 100) if event_counter > 0 else 0

# print("="*50)
# print("PERFORMANCE METRICS")
# print("="*50)
# print(f"Total Events Sent: {event_counter}")
# print(f"Successful: {event_counter - error_counter}")
# print(f"Errors: {error_counter}")
# print(f"Success Rate: {success_rate:.2f}%")
# print(f"Execution Time: {execution_time:.2f} seconds")
# print(f"Throughput: {events_per_second:.2f} events/second")
# print(f"Average Latency: {(execution_time/event_counter)*1000:.2f} ms/event" if event_counter > 0 else "N/A")
# print("-"*50)
# print(f"Initial Memory: {initial_memory:.2f} MB")
# print(f"Final Memory: {final_memory:.2f} MB")
# print(f"Memory Used: {memory_used:.2f} MB")
# print(f"CPU Usage: {cpu_percent:.2f}%")
# print("-"*50)
# print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# print("="*50)

# # Display partition distribution
# if partition_stats:
#     print("\n" + "="*50)
#     print("PARTITION DISTRIBUTION")
#     print("="*50)
#     for mapping, count in sorted(partition_stats.items(), key=lambda x: x[1], reverse=True):
#         percentage = (count / event_counter * 100) if event_counter > 0 else 0
#         print(f"{mapping}: {count:,} events ({percentage:.1f}%)")
#     print("="*50)
=======
>>>>>>> 8cf936542ea0f190c64b627131ce5e676cc3e419
