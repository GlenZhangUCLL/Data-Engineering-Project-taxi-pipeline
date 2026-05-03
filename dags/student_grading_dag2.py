#code here
import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
#from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.python import PythonSensor

# Configuration
RAW_DIR = "/tmp/raw/"
OUT_DIR = "/tmp/output/"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

# Extraction Logic 
def extract_all():
    # Students Data
    pd.DataFrame({
        'student': ['Alice', 'Bob', 'Charlie'],
        'score': [18, 12, 15] # Charlie has a missing score
    }).to_csv(f"{RAW_DIR}students.csv", index=False)
    
    # Sales Data
    pd.DataFrame({
        'item': ['Laptop', 'Mouse', 'Keyboard'],
        'price': [1200, 25, 10] # Keyboard has a negative price -> invalid
    }).to_csv(f"{RAW_DIR}sales.csv", index=False)
    print("All data extracted to raw folder.")

# Validation Logic
# def validate_data(file_name, column_to_check):
#     df = pd.read_csv(f"{RAW_DIR}{file_name}")
#     if df[column_to_check].isnull().any():
#         raise ValueError(f"Quality Check Failed: Null values found in {file_name}!")
#     if (df[column_to_check] < 0).any():
#         raise ValueError(f"Quality Check Failed: Negative values found in {file_name}!")
#     print(f"Validation passed for {file_name}.")

def validate_data(file_name, column_to_check):
    df = pd.read_csv(f"{RAW_DIR}{file_name}")
    errors = [] # Create a container for errors

    # Check 1: Nulls
    if df[column_to_check].isnull().any():
        errors.append(f"Null values found in {file_name}!")

    # Check 2: Negatives
    if (df[column_to_check] < 0).any():
        errors.append(f"Negative values found in {file_name}!")

    # If the list is not empty, raise one big error
    if errors:
        raise ValueError(f"Quality Check Failed: {', '.join(errors)}")
    
    print(f"Validation passed for {file_name}.")

# Data Processing 
def process_data(file_name):
    df = pd.read_csv(f"{RAW_DIR}{file_name}")
    df['processed_at'] = datetime.now()
    df.to_csv(f"{OUT_DIR}processed_{file_name}", index=False)
    print(f"File {file_name} processed and saved.")

def check_for_file():
    return os.path.exists('/tmp/raw/students.csv')

with DAG(
    dag_id="master_data_orchestration",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    max_active_runs=1,
    catchup=False
) as dag:

    # start = EmptyOperator(task_id="start_pipeline")
    wait_for_file= PythonSensor(
        task_id='watch_for_new_data',
        python_callable=check_for_file,
        poke_interval=5,
        timeout= 60*30,
        mode='poke'
    )
    
    extract = PythonOperator(
        task_id="extract_all_data",
        python_callable=extract_all
    )

    #Student validation
    val_students = PythonOperator(
        task_id="validate_students",
        python_callable=validate_data,
        op_args=["students.csv", "score"]
    )
    proc_students = PythonOperator(
        task_id="process_students",
        python_callable=process_data,
        op_args=["students.csv"]
    )

    #Sales validation 
    val_sales = PythonOperator(
        task_id="validate_sales",
        python_callable=validate_data,
        op_args=["sales.csv", "price"]
    )
    proc_sales = PythonOperator(
        task_id="process_sales",
        python_callable=process_data,
        op_args=["sales.csv"]
    )

    # Final "Finish" node - trigger_rule="all_done" so it runs even if some branches fail
    finish = EmptyOperator(
        task_id="finish_pipeline",
        trigger_rule="all_done"
    )

    # Dependency Flow 
    wait_for_file >> extract
    
    # Fan out to parallel processing
    extract >> val_students >> proc_students >> finish
    extract >> val_sales >> proc_sales >> finish