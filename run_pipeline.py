"""
Orchestrates the Spark ETL job using Prefect.
"""
import os
from prefect import flow, task
from spark_etl_job import main as run_spark_etl

@task(name="Run Spark ETL Job")
def run_spark_etl_task():
    """
    Prefect task to execute the main function from spark_etl_job.py.
    """
    run_spark_etl()

@flow(name="Housing ETL Pipeline")
def housing_etl_pipeline():
    """
    The main Prefect flow that orchestrates the ETL process.
    """
    print("--- Starting Housing ETL Pipeline ---")
    # Check for DB password, as it's required by the Spark job
    if not os.getenv("DB_PASSWORD"):
        raise ValueError("Environment variable DB_PASSWORD is not set.")
    
    run_spark_etl_task()
    print("--- Housing ETL Pipeline Finished ---")

if __name__ == "__main__":
    housing_etl_pipeline()
