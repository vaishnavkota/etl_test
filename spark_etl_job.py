"""
PySpark ETL Job
----------------
This script defines a PySpark ETL job for processing the California Housing dataset.
The job reads data from a CSV file, performs transformations to aggregate housing metrics,
and writes the results to a SQLite database. It is designed to be executed directly
or orchestrated by a workflow manager like Prefect.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, current_timestamp, count
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
import os
import time
# import findspark

# --- Configuration ---
# SQLite
DB_NAME = "housing_data.db"

CSV_FILE_PATH = "california_housing_test.csv"

def read_data(spark):
    start_read = time.time()
    
    if os.path.exists(CSV_FILE_PATH):
        print(f">>> Reading data from {CSV_FILE_PATH}...")
        schema = StructType([
            StructField("longitude", FloatType(), nullable=True),
            StructField("latitude", FloatType(), nullable=True),
            StructField("housing_median_age", FloatType(), nullable=True),
            StructField("total_rooms", FloatType(), nullable=True),
            StructField("total_bedrooms", FloatType(), nullable=True),
            StructField("population", FloatType(), nullable=True),
            StructField("households", FloatType(), nullable=True),
            StructField("median_income", FloatType(), nullable=True),
            StructField("median_house_value", FloatType(), nullable=True)
        ])
        df = spark.read.option("header", "true").schema(schema).csv(CSV_FILE_PATH)

        # Metrics
        print(f"\n--- READ METRICS ---")
        print(f"Time Taken: {time.time() - start_read:.4f}s")
        print("--------------------\n")
        return df
    else:
        print(">>> No data source found. Exiting.")
        return None

def transform_data(df):
    if df is None: return None
    print(">>> Transforming Housing Data...")
    


    df_agg = df.groupBy("housing_median_age") \
        .agg(
            avg("median_house_value").alias("avg_house_value"),
            _sum("population").alias("total_population"),
            _sum("total_rooms").alias("total_rooms_in_age_group"),
            count("households").alias("district_count")
        ) \
        .withColumn("processed_at", current_timestamp())
    return df_agg

def write_to_sqlite(df):
    if df is None:
        return
    # JDBC URL for SQLite
    jdbc_url = f"jdbc:sqlite:{DB_NAME}"
    props = {"driver": "org.sqlite.JDBC"}
    try:
        df.write.jdbc(url=jdbc_url, table="housing_age_metrics", mode="overwrite", properties=props)
        print(">>> SQLite Write Successful.")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")

def main():
    job_start = time.time()
    # findspark.init()
    spark = SparkSession.builder \
        .appName("California_Housing_ETL")\
        .master("local[1]")\
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.42.0.0") \
        .getOrCreate()
    try:
        raw_df = read_data(spark)
        if raw_df:
            metrics_df = transform_data(raw_df)
            if metrics_df:
                metrics_df.show(5)
                write_to_sqlite(metrics_df)
    except Exception as e:
        print(f"An error occurred during the ETL job: {e}")
        raise
    finally:
        print(f"\n>>> Total Execution Time: {time.time() - job_start:.4f}s <<<")
        

if __name__ == "__main__":
    main()