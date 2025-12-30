
#!/bin/bash
echo "Waiting for MongoDB to start..."
sleep 20

echo "1. Setting up Config Server..."
mongosh --host mongo-config:27019 --eval '
rs.initiate({
  _id: "configrs",
  configsvr: true,
  members: [{ _id: 0, host: "mongo-config:27019" }]
});
' || true
sleep 5

echo "2. Setting up Shard 1..."
mongosh --host mongo-shard1:27018 --eval '
rs.initiate({
  _id: "shard1rs",
  members: [{ _id: 0, host: "mongo-shard1:27018" }]
});
' || true
sleep 5

echo "3. Setting up Shard 2..."
mongosh --host mongo-shard2:27018 --eval '
rs.initiate({
  _id: "shard2rs",
  members: [{ _id: 0, host: "mongo-shard2:27018" }]
});
' || true
sleep 5

echo "4. Setting up Shard 3..."
mongosh --host mongo-shard3:27018 --eval '
rs.initiate({
  _id: "shard3rs",
  members: [{ _id: 0, host: "mongo-shard3:27018" }]
});
' || true
sleep 15

echo "5. Adding shards to cluster..."
mongosh --host mongos:27017 --eval '
sh.addShard("shard1rs/mongo-shard1:27018");
sh.addShard("shard2rs/mongo-shard2:27018");
sh.addShard("shard3rs/mongo-shard3:27018");
' || true
sleep 5

echo "6. Creating database and collection..."
mongosh --host mongos:27017 --eval '
sh.enableSharding("taxi_streaming");
use taxi_streaming;
db.createCollection("taxi_events");
db.taxi_events.createIndex({ trip_id: 1 }, { unique: true });
db.taxi_events.createIndex({ pickup_datetime: -1 });
db.taxi_events.createIndex({ pickup_borough: 1 });
db.taxi_events.createIndex({ pickup_borough_id: 1 });
sh.shardCollection("taxi_streaming.taxi_events", { pickup_borough_id: 1 });
' || true

echo "✅ Setup complete!"
mongosh --host mongos:27017 --quiet --eval 'sh.status()'
