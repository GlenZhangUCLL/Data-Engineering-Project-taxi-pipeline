from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os


#  Configurations 
DATA_DIR = "/opt/airflow/data"
INPUT_FOLDER = f"{DATA_DIR}/input_monitor"
LOCAL_OUTPUT_FOLDER = f"{DATA_DIR}/output_local"
# The sensor looks for any CSV in the monitor folder
TARGET_FILE_PATTERN = "dirty_taxi_feb.csv"

# Intermediate files
PROCESSING_FILE = f"{DATA_DIR}/temp_processing.csv"

#  Task Functions 
def reader_task():
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)

    file_path = f"{INPUT_FOLDER}/{TARGET_FILE_PATTERN}"
    print(f"Reading {file_path}...")
    df = pd.read_csv(file_path)

    #Handle empty files
    if df.empty:
        raise ValueError("The uploaded CSV is empty! Stopping pipeline.")
    
    df.to_csv(PROCESSING_FILE, index=False)
    print(f"Loaded {len(df)} rows.")

def validator_task():
    print("Running Detailed Validation...")
    df = pd.read_csv(PROCESSING_FILE)
    total_rows = len(df)

    # 1. Handle the "ERROR" strings in numeric columns
    # errors='coerce' turns "ERROR" into NaN
    df['trip_distance'] = pd.to_numeric(df['trip_distance'], errors='coerce')
    df['fare_amount'] = pd.to_numeric(df['fare_amount'], errors='coerce')

    # 2. Rule-based cleaning
    initial_count = len(df)
    # Drop rows where critical values are NaN 
    df = df.dropna(subset=['passenger_count', 'trip_distance', 'fare_amount'])

    # Drop rows with impossible values 
    df = df[(df['fare_amount'] > 0) & (df['trip_distance'] > 0) & (df['trip_distance'] < 200)]

    # 3. Generate Real-time quality report
    clean_rows = len(df)
    health_score = (clean_rows / total_rows * 100) if total_rows > 0 else 0

    report_path = f"{DATA_DIR}/rt_quality_report.txt"
    with open(report_path, "w") as f:
        f.write(f"REAL-TIME AUDIT | {datetime.now()}\n")
        f.write(f"Health Score: {health_score:.2f}%\n")
        f.write(f"Rows Dropped: {total_rows - clean_rows}\n")

    print(f"Validation complete. Health Score: {health_score:.2f}%")
    df.to_csv(PROCESSING_FILE, index=False)

def processor_task():
    print("Processing: Removing duplicates and adding features...")
    df = pd.read_csv(PROCESSING_FILE)

    # 1. Remove duplicates
    df = df.drop_duplicates()

    # 2. Adds 3 columns
    
    # Feature 1: Tip Percentage
    df['tip_percentage'] = (df['tip_amount']/df['fare_amount'].replace(0, np.nan) * 100).fillna(0).round(2)

    # Feature 2: High Value Trip Flag
    df['is_high_value'] = df['total_amount'] > 50

    # Feature 3: Trip Duration (minutes)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() /60

    df.to_csv(PROCESSING_FILE, index=False)
    print("Processing complete.")

def writer_task():
    df = pd.read_csv(PROCESSING_FILE)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    filename = f"cleaned_feb_data_{timestamp}.csv"

    # 1. Write to local output folder
    if not os.path.exists(LOCAL_OUTPUT_FOLDER):
        os.makedirs(LOCAL_OUTPUT_FOLDER) 

    local_path = f"{LOCAL_OUTPUT_FOLDER}/{filename}"
    df.to_csv(local_path, index=False) 
    print(f"Saved locally to {local_path}")


    # 2. Write to Azure Blob Storage
    try:
        hook = WasbHook(wasb_conn_id='azure_blob_conn')
        container_name = 'taxi-data'

        #Upload data
        hook.load_file(file_path = local_path, container_name = container_name, blob_name = filename, overwrite=True)
        print(f"Uploaded to Azure as {filename}")

        #Upload report
        report_path = f"{DATA_DIR}/rt_quality_report.txt"
        hook.load_file(file_path=report_path, container_name=container_name, blob_name=f"audit_{timestamp}.txt")

    except Exception as e:
        print(f"Upload Azure failed: {e}")
        raise

def cleanup_task():
    file_path = f"{INPUT_FOLDER}/{TARGET_FILE_PATTERN}"
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Successfully deleted {TARGET_FILE_PATTERN} to prevent double-processing.")
    else:
        print("File already removed.")

    #  DAG Definition 

default_args = {
    'owner': 'airflow',
    'retries': 0
}

with DAG(
    dag_id ='realtime_taxi_pipeline',
    default_args = default_args,
    start_date = datetime(2026, 5, 1),
    schedule = timedelta(minutes=1),
    catchup = False
) as dag:
        
    # Monitors the input folder
    monitor_folder = FileSensor(
        task_id='monitor_input_folder',
        filepath=TARGET_FILE_PATTERN,
        fs_conn_id='fs_default',
        poke_interval=10,  #Check every 10s
        timeout=60        #Give up after 10 mins if no file found
    )

    t1 = PythonOperator(task_id='read_csv', python_callable=reader_task)
    t2 = PythonOperator(task_id='validate_data', python_callable=validator_task)
    t3 = PythonOperator(task_id='process_data', python_callable=processor_task)
    t4 = PythonOperator(task_id='write_csv', python_callable=writer_task)
    t5 = PythonOperator(task_id='cleanup_input_file', python_callable=cleanup_task)

    monitor_folder >> t1 >> t2 >> t3 >> t4 >> t5
         