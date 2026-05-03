# NYC Taxi Data Engineering Pipeline 


---

##  Repository Structure


*   `dags/`: Contains the Airflow DAGs for the Batch and Real-time pipelines.
*   `scripts/`: Includes `generate_dirty_data.py`, a utility script used to sample raw Parquet data and inject intentional anomalies (NaNs, negative values, type errors).
*   `samples/`: Stores `dirty_taxi_feb.csv`, a 150-row "corrupted" dataset used to verify that the pipeline's validation and error-logging tasks work as intended.
*   `config/` & `plugins/`: Custom Airflow configurations and extension points.
*   `requirements.txt`: Lists all Python dependencies, including Azure providers and data processing libraries.

---

##  Data Quality & Testing

To simulate real-world data engineering challenges, I developed a custom data-injection process:
1.  Source: NYC TLC Yellow Taxi Trip Records (February 2025).
2.  Simulation: The `scripts/generate_dirty_data.py` script was used to create a test payload by:
    *   Injecting Null values into the `passenger_count`.
    *   Inserting negative fares into the `fare_amount`.
    *   Adding string errors ("ERROR") into numeric columns.
    *   Creating duplicate records.

The pipeline is designed to catch these anomalies, log them in a quality report, and prevent corrupted data from reaching the final production table.

---

##  Getting Started

### 1. Environment Setup
Clone the repository and install the necessary Python libraries:
```bash
pip install -r requirements.txt
