from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pandas as pd
import os
import numpy as np

# Configurations 
DATA_DIR = "/opt/airflow/data" 
INPUT_FILE = f"{DATA_DIR}/yellow_tripdata_2025-01.parquet"

# Intermediate "checkpoint" files
RAW_OUTPUT = f"{DATA_DIR}/raw_data.parquet"
VALID_OUTPUT = f"{DATA_DIR}/validated_data.parquet"
PROCESSED_OUTPUT = f"{DATA_DIR}/processed_data.parquet"

MANDATORY_COLS = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'PULocationID', 'DOLocationID', 
    'payment_type', 'fare_amount', 'total_amount'
]

#  Task Functions 

def reader_task():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Reading data from {INPUT_FILE}...")
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Missing source file at {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    df.to_parquet(RAW_OUTPUT)
    print(f"Initial load complete: {len(df)} rows.")

def validator_task():
    print("Running Primary Validation...")
    df = pd.read_parquet(RAW_OUTPUT)
    total_rows = len(df)

    # Cast types
    df['fare_amount'] = pd.to_numeric(df['fare_amount'], errors='coerce')
    df['trip_distance'] = pd.to_numeric(df['trip_distance'], errors='coerce')
    
    # Check columns
    for col in MANDATORY_COLS:
        if col not in df.columns:
            raise ValueError(f"Missing mandatory column: {col}")
            
    # Drop nulls 
    clean_df = df.dropna(subset=MANDATORY_COLS)

    # Consistency and positive check
    clean_df = clean_df[(clean_df['trip_distance'] > 0) & (clean_df['trip_distance'] < 200)]

    # EXTRA: Generate a data quality report
    clean_rows = len(clean_df)
    dropped_rows = total_rows - clean_rows
    health_score = (clean_rows / total_rows * 100) if total_rows > 0 else 0

    report_path = f"{DATA_DIR}/quality_report.txt"
    with open(report_path, "w") as f:
        f.write("--- NYC TAXI PIPELINE QUALITY AUDIT ---\n")
        f.write(f"Timestamp: {datetime.now()}\n")
        f.write(f"Total Rows Analyzed: {total_rows}\n")
        f.write(f"Valid Rows: {clean_rows}\n")
        f.write(f"Anomalies Dropped: {dropped_rows}\n")
        f.write(f"Data Health Score: {health_score:.2f}%\n")
    
    clean_df.to_parquet(VALID_OUTPUT)
    print(f"Validation complete: {len(clean_df)} rows remaining.")

def processor_task():
    print("Processing data...")
    df = pd.read_parquet(VALID_OUTPUT)
    
    # Create trip_duration
    df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

    # Handling Non-Mandatory Columns (Filling missing amounts with 0)
    non_mandatory = [
        'tip_amount', 'tolls_amount', 'extra', 'airport_fee', 
        'congestion_surcharge', 'cbd_congestion_fee'
    ]
    for col in non_mandatory:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # 1. Remove specific columns
    cols_to_remove = ['VendorID', 'store_and_fwd_flag', 'RatecodeID']
    df = df.drop(columns=[c for c in cols_to_remove if c in df.columns])

    # 2. Add Time Features
    df['pickup_year'] = df['tpep_pickup_datetime'].dt.year
    df['pickup_month'] = df['tpep_pickup_datetime'].dt.month

    # 3. Calculated Metrics
    
    # Speed calculation
    df['average_speed_mph'] = (df['trip_distance'] / (df['trip_duration_min'] / 60).replace(0, np.nan)).fillna(0)

    # Revenue calculation
    df['revenue_per_mile'] = (df['total_amount'] / df['trip_distance'].replace(0, np.nan)).fillna(0)

    # 4. Trip Distance Category
    
    df['trip_distance_category'] = pd.cut(
        df['trip_distance'],
        bins=[-np.inf, 2, 10, np.inf],
        labels=['Short', 'Medium','Long']
    )

    # 5. Fare Category
    df['fare_category'] = pd.cut(
        df['total_amount'],
        bins=[-np.inf, 20, 50, np.inf],
        labels=['Low', 'Medium','High']
    )

    # 6. Time of Day

    hour = df['tpep_pickup_datetime'].dt.hour
    
    # Prefill with evening as default
    df['trip_time_of_day'] = 'Evening'

    # Correct all the default with the correct category
    df.loc[hour < 6, 'trip_time_of_day'] = 'Night'
    df.loc[(hour >= 6) & (hour < 12), 'trip_time_of_day'] = 'Morning'
    df.loc[(hour >= 12) & (hour < 18), 'trip_time_of_day'] = 'Afternoon'
    
    df.to_parquet(PROCESSED_OUTPUT)

def backup_validator_task():
    print("Running Back-up Validation...")
    df = pd.read_parquet(PROCESSED_OUTPUT)
    
    # Check duration realism
    final_df = df[(df['trip_duration_min'] > 0) & (df['trip_duration_min'] < 1440)]
    
    # Final Write inside this task or a separate one
    output_path = f"{DATA_DIR}/cleaned_taxi_data_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    final_df.to_parquet(output_path)
    print(f"Success! Final data saved to {output_path}")

    # 2. Azure Write
    print("Uploading to Azure...")
    try:
        hook = WasbHook(wasb_conn_id='azure_blob_conn')
        container_name = "taxi-data" 
        blob_name = os.path.basename(output_path)
        
        #Upload data
        hook.load_file(file_path=output_path, container_name=container_name, blob_name=blob_name, overwrite=True)
        
        # EXTRA: upload report 
        report_path = f"{DATA_DIR}/quality_report.txt"
        if os.path.exists(report_path):
            hook.load_file(file_path=report_path, container_name=container_name, blob_name="quality_report_january.txt", overwrite=True)

        print(f"Success! Data uploaded to Azure as {blob_name}")
    except Exception as e:
        print(f"Azure upload failed: {e}")
        raise
    


#  DAG Definition 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='yellow_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi Batch Processing Pipeline',
    # SET YOUR DEFENSE DATE HERE:
    start_date=datetime(2026, 4, 1), 
    schedule='@once',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='read_data', python_callable=reader_task)
    t2 = PythonOperator(task_id='validate_mandatory_cols', python_callable=validator_task)
    t3 = PythonOperator(task_id='process_features', python_callable=processor_task)
    t4 = PythonOperator(task_id='backup_validation_and_write', python_callable=backup_validator_task)

    # Setting the sequence
    t1 >> t2 >> t3 >> t4