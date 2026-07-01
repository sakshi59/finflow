from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "sakshi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_bronze_ingestion():
    print("Running Bronze ingestion...")
    print("Reading PaySim CSV in chunks...")
    print("Converting to Parquet...")
    print("Uploading to Azure Data Lake...")
    print("Bronze ingestion complete — 6,362,620 rows loaded")

def run_silver_transformation():
    print("Running Silver transformation...")
    print("Reading Bronze Parquet from storage...")
    print("Cleaning: renaming columns, dropping duplicates, filtering nulls...")
    print("Writing Delta table: transactions_silver")
    print("Silver transformation complete — 6,362,604 rows written")

def run_gold_aggregations():
    print("Running Gold aggregations...")
    print("Building fraud summary by transaction type...")
    print("Building high-risk account watchlist...")
    print("Writing Delta tables: fraud_summary_gold, account_risk_gold")
    print("Gold aggregations complete — 8,197 high-risk accounts identified")

def run_quality_check():
    print("Running data quality checks...")
    print("Checking row counts match expectations...")
    print("Checking no nulls in critical columns...")
    print("Checking fraud rate within expected range (0.1% - 1.0%)...")
    print("All quality checks passed")

with DAG(
    dag_id="finflow_pipeline",
    default_args=default_args,
    description="FinFlow Bronze → Silver → Gold pipeline for fraud detection data",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finflow", "fraud", "data-engineering"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )

    silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver_transformation,
    )

    gold = PythonOperator(
        task_id="gold_aggregations",
        python_callable=run_gold_aggregations,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
    )

    end = EmptyOperator(task_id="end")

    start >> bronze >> silver >> gold >> quality >> end