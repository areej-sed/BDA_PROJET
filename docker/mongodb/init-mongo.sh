#!/bin/bash

echo "Initializing MongoDB Cluster..."

# Initialize Config Server
docker exec mongo-config mongosh --port 27019 --eval "rs.initiate({_id:'configrs',configsvr:true,members:[{_id:0,host:'mongo-config:27019'}]})"
sleep 10

# Initialize Shards
docker exec mongo-shard1 mongosh --port 27018 --eval "rs.initiate({_id:'shard1rs',members:[{_id:0,host:'mongo-shard1:27018'}]})"
docker exec mongo-shard2 mongosh --port 27018 --eval "rs.initiate({_id:'shard2rs',members:[{_id:0,host:'mongo-shard2:27018'}]})"
docker exec mongo-shard3 mongosh --port 27018 --eval "rs.initiate({_id:'shard3rs',members:[{_id:0,host:'mongo-shard3:27018'}]})"
sleep 15

# Add Shards
docker exec mongos mongosh --eval "sh.addShard('shard1rs/mongo-shard1:27018')"
docker exec mongos mongosh --eval "sh.addShard('shard2rs/mongo-shard2:27018')"
docker exec mongos mongosh --eval "sh.addShard('shard3rs/mongo-shard3:27018')"

# Enable Sharding
docker exec mongos mongosh --eval "sh.enableSharding('taxi_streaming')"

sleep 5

# Enable Balancer and Check Status
echo "🔹 Starting Balancer..."
docker exec mongos mongosh --eval "sh.startBalancer()"
sleep 3
docker exec mongos mongosh --eval "sh.getBalancerState()"

# Create Collection and Shard with HASHED key
# docker exec mongos mongosh --eval "
#   db = db.getSiblingDB('taxi_streaming');
#   db.createCollection('taxi_events');
#   sh.shardCollection('taxi_streaming.taxi_events', {pickup_borough_id: 'hashed'});
# "

# Create Additional Indexes (after sharding)
# docker exec mongos mongosh --eval "
#   db = db.getSiblingDB('taxi_streaming');
#   db.taxi_events.createIndex({trip_id: 1});
#   db.taxi_events.createIndex({pickup_datetime: 1});
#   db.taxi_events.createIndex({pickup_borough_id: 1});
# "

echo "🔹 Running mongo-init.js..."
docker exec -i mongos mongosh < /mongo-init.js


sleep 15

echo "Done! Verifying..."
docker exec mongos mongosh --eval "use taxi_streaming; db.taxi_events.getIndexes()"
docker exec mongos mongosh --eval "sh.status()"






# #!/bin/sh

# echo "======================================"
# echo "🚀 Initializing MongoDB Sharded Cluster"
# echo "======================================"

# ########################################
# # 1️⃣ Init Config Server Replica Set
# ########################################
# echo "🔹 Initializing Config Server..."
# docker exec mongo-config mongosh --port 27019 --eval '
# rs.initiate({
#   _id: "configrs",
#   configsvr: true,
#   members: [{ _id: 0, host: "mongo-config:27019" }]
# })
# '
# sleep 10

# ########################################
# # 2️⃣ Init Shard Replica Sets
# ########################################
# echo "🔹 Initializing Shard 1..."
# docker exec mongo-shard1 mongosh --port 27018 --eval '
# rs.initiate({
#   _id: "shard1rs",
#   members: [{ _id: 0, host: "mongo-shard1:27018" }]
# })
# '

# echo "🔹 Initializing Shard 2..."
# docker exec mongo-shard2 mongosh --port 27018 --eval '
# rs.initiate({
#   _id: "shard2rs",
#   members: [{ _id: 0, host: "mongo-shard2:27018" }]
# })
# '

# echo "🔹 Initializing Shard 3..."
# docker exec mongo-shard3 mongosh --port 27018 --eval '
# rs.initiate({
#   _id: "shard3rs",
#   members: [{ _id: 0, host: "mongo-shard3:27018" }]
# })
# '

# sleep 15

# ########################################
# # 3️⃣ Add Shards to Cluster
# ########################################
# echo "🔹 Adding shards to mongos..."
# docker exec mongos mongosh --eval 'sh.addShard("shard1rs/mongo-shard1:27018")'
# docker exec mongos mongosh --eval 'sh.addShard("shard2rs/mongo-shard2:27018")'
# docker exec mongos mongosh --eval 'sh.addShard("shard3rs/mongo-shard3:27018")'

# sleep 10

# ########################################
# # 4️⃣ Enable Sharding on Database
# ########################################
# echo "🔹 Enabling sharding on database..."
# docker exec mongos mongosh --eval 'sh.enableSharding("taxi_streaming")'

# sleep 5

# ########################################
# # 5️⃣ Run MongoDB JS Init Script
# ########################################
# echo "🔹 Running mongo-init.js..."
# docker exec -i mongos mongosh < /mongo-init.js

# ########################################
# # 6️⃣ Verification
# ########################################
# echo "🔍 Verifying indexes..."
# docker exec mongos mongosh --eval '
# use taxi_streaming;
# db.taxi_events.getIndexes();
# '

# echo "🔍 Sharding status..."
# docker exec mongos mongosh --eval 'sh.status()'

# echo "======================================"
# echo "✅ MongoDB Cluster Initialized Successfully"
# echo "======================================"
