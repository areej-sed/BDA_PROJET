import pandas as pd 
import geopandas as gpd 

input_path = "../data/input/"
output_path = "../data/output/"

main_data = pd.read_parquet(input_path + "yellow_tripdata_2024_01.parquet")
main_data['PULocationID'].nunique() 
main_data['DOLocationID'].nunique()

# Read the shapefile
gdf = gpd.read_file(input_path + "zones/taxi_zones.shp")

# Extract latitude and longitude separately
gdf['latitude'] = gdf.geometry.centroid.y
gdf['longitude'] = gdf.geometry.centroid.x
gdf_join = gdf[['LocationID', 'zone', 'borough', 'latitude', 'longitude']]
# Join with pickup location
main_data = main_data.merge(gdf_join.rename(columns={'LocationID': 'PULocationID', 
                                                       'zone': 'pickup_zone', 
                                                       'borough': 'pickup_borough', 
                                                       'latitude': 'pickup_latitude', 
                                                       'longitude': 'pickup_longitude'}), 
                            on='PULocationID', how='left')

# Join with dropoff location
main_data = main_data.merge(gdf_join.rename(columns={'LocationID': 'DOLocationID', 
                                                       'zone': 'dropoff_zone', 
                                                       'borough': 'dropoff_borough', 
                                                       'latitude': 'dropoff_latitude', 
                                                       'longitude': 'dropoff_longitude'}), 
                            on='DOLocationID', how='left')
# Rename datetime columns
main_data = main_data.rename(columns={
    'tpep_pickup_datetime': 'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime'
})
best_columns = ['pickup_datetime', 'dropoff_datetime',
       'passenger_count', 'trip_distance',
       'payment_type', 'fare_amount', 'total_amount', 
       'pickup_zone', 'pickup_borough', 'pickup_latitude', 'pickup_longitude', 
       'dropoff_zone', 'dropoff_borough', 'dropoff_latitude', 'dropoff_longitude']

final_main_data = main_data[best_columns]



# Add id column
final_main_data.insert(0, 'trip_id', range(1, len(final_main_data) + 1))

# Update null values
final_main_data = final_main_data.fillna({
    'pickup_borough': 'unknown',
    'pickup_zone': 'unknown',
    'pickup_latitude': 0.0,
    'pickup_longitude': 0.0,
    'dropoff_borough': 'unknown',
    'dropoff_zone': 'unknown',
    'dropoff_latitude': 0.0,
    'dropoff_longitude': 0.0,
    'passenger_count': 0,
    'trip_distance': 0.0,
    'fare_amount': 0.0,
    'total_amount': 0.0
})


# Save as JSON
final_main_data.to_json(output_path + 'final_data.json', orient='records', indent=2 , date_format='iso')

print(f"File saved to {output_path}final_data.json")
print(f"Total records: {len(final_main_data)}")
