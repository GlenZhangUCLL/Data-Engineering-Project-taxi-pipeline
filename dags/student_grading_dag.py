from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

RAW_PATH = "/tmp/raw_data.csv"
CLEAN_PATH = "/tmp/clean_data.csv"

def generate_data():
    # Generate a simple dataframe, for example students and their scores on 20 for data engineering but leave some NA values. 
    # Store this as a csv on the raw path
    print(f"Generating data at {RAW_PATH}...")
    
    data = {
        'student': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [18, None, 15, 9, None]
    }
    df = pd.DataFrame(data)

    #Ensure that the directory exists
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    df.to_csv(RAW_PATH, index=False)
    print("Raw data generated successfully")

def clean_data():
    #clean the generated csv and store this on the clean path (try experiment already with azure)
        print(f"Cleaning data at {RAW_PATH}...")
        if not os.path.exists(RAW_PATH):
              print("No file found")
              return
        
        df = pd.DataFrame(pd.read_csv(RAW_PATH))

        # df['score'] = df['score'].fillna(0)
        df.to_csv(CLEAN_PATH, index=False)
        print(f"Cleaned data saved to {CLEAN_PATH}.")
    

# Create a dag to run these tasks, try to let this task run at 30 april at 9 AM
with DAG(
    # code here
    dag_id="student_score_pipline",
    start_date=datetime(2024, 4, 30, 9, 0), # Set to April 30th
    schedule="0 9 30 4 *",                # Cron for 9:00 AM on April 30th
    catchup=False
    
) as dag:
    
    # code here
    task_generate= PythonOperator(
          task_id = "generate_student_data",
          python_callable = generate_data
    )

    task_clean = PythonOperator(
          task_id = "clean_student_data",
          python_callable  = clean_data
    )

    task_generate >> task_clean