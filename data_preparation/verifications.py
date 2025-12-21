import json

with open("data/nyc_taxi_events.json") as f:
    for i, line in enumerate(f):
        json.loads(line)
        if i == 2824462:
            break
print("JSON valide")
