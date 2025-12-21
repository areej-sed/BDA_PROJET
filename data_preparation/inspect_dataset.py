import pandas as pd

df = pd.read_parquet("data/yellow_tripdata_2024_01.parquet")

print(df.columns)
print(df.head())
print(len(df))
