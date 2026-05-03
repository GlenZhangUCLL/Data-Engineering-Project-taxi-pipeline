import pandas as pd
import numpy as np
import os

# 1. Setup paths relative to the project root
input_parquet = "data/yellow_tripdata_2025-02.parquet" 
output_csv = "samples/dirty_taxi_feb.csv"

# 2. Ensure the output directory exists
os.makedirs("samples", exist_ok=True)

if not os.path.exists(input_parquet):
    print(f"Error: {input_parquet} not found. Please place the raw parquet file in the data/ folder.")
else:
    # 3. Load and Select Columns (Crucial for your Airflow DAG)
    df = pd.read_parquet(input_parquet)
    cols = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'PULocationID', 'DOLocationID', 
        'payment_type', 'fare_amount', 'tip_amount', 'total_amount'
    ]
    
    # Take a 150-row sample with only the needed columns
    df_dirty = df.head(150)[cols].copy()

    # 4. Inject "The Dirty Stuff"
    # Allow this column to hold strings for the "ERROR" injection
    df_dirty['trip_distance'] = df_dirty['trip_distance'].astype(object)
    
    df_dirty.loc[0:2, 'passenger_count'] = np.nan   # Missing values
    df_dirty.loc[5, 'fare_amount'] = -10.5          # Negative value
    df_dirty.loc[10, 'trip_distance'] = "ERROR"     # Wrong data type
    df_dirty = pd.concat([df_dirty, df_dirty.iloc[[20]]]) # Duplicate row

    # 5. Save as the dirty CSV
    df_dirty.to_csv(output_csv, index=False)
    print(f"Successfully generated {output_csv} with {len(df_dirty)} rows.")