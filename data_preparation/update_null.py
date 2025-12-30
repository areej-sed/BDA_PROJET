import json

# Charger les données depuis le fichier JSON
with open("../data/output/final_data.json", "r") as f:
    events = json.load(f)

# Modifier les trips où pickup_borough est null en "unknown"
modified_zone = 0
modified_count = 0
for event in events:
    if event.get("pickup_borough") is None:
        event["pickup_borough"] = "unknown"
        event["pickup_zone"] = "unknown"
        event["pickup_latitude"] = 0.0
        event["pickup_longitude"] = 0.0
        modified_zone += 1
    if event.get("passenger_count") is None:
        event["passenger_count"] = 0  
        modified_count += 1

# Afficher le nombre de modifications
print(f"Nombre de trips avec zones modifiés : {modified_zone}")
print(f"Nombre de trips avec passenger_count modifié : {modified_count}")

# Sauvegarder les données modifiées dans un nouveau fichier JSON
with open("../data/output/final_data_updated.json", "w") as f:
    json.dump(events, f, indent=4)

print("Les données ont été sauvegardées dans 'final_data_updated.json'")