print("🔹 Switching to database...");
db = db.getSiblingDB("taxi_streaming");

print("🔹 Creating collection...");
db.createCollection("taxi_events");


print("🔹 Creating indexes...");
db.taxi_events.createIndex({ trip_id: 1 });
db.taxi_events.createIndex({ pickup_datetime: 1 });
db.taxi_events.createIndex({ pickup_borough_id: 1 });

print("🔹 Sharding collection...");
sh.shardCollection(
  "taxi_streaming.taxi_events",
  { pickup_borough_id: "hashed" }
);

print("✅ MongoDB initialization completed successfully");
