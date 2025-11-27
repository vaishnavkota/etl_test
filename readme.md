# Data Engineering Pipelines: Housing & Arxiv Analysis

This repository contains two distinct ETL (Extract, Transform, Load) pipelines demonstrating modern data engineering practices using PySpark, MongoDB, MySQL, and Airflow.

---

## 🏗️ Pipeline 1: California Housing ETL
**Goal:** Process housing data to analyze metrics by housing age.

### Architecture
1.  **Extract**: Reads from **MongoDB** (primary) or falls back to **CSV** (`california_housing_test.csv`).
2.  **Transform**: Aggregates **Average House Value** and **Population** grouped by **Housing Age**.
3.  **Load**: Saves results to:
    *   **MySQL**: `housing_age_metrics` table.
    *   **MongoDB**: `housing_metrics` collection.
4.  **Orchestration**: Managed by **Apache Airflow**.

### Key Files
*   `spark_etl_job.py`: Main Spark application for housing data.
*   `test_etl_logic.py`: Unit tests for data transformation logic.
*   `init_db.sql`: SQL script to initialize the MySQL database.
*   `housing_pipeline_dag.py`: Airflow DAG for scheduling.

---

## 🚀 Pipeline 2: MongoDB Arxiv ETL (New)
**Goal:** High-performance loading and partitioning of large JSON datasets (Arxiv Metadata) into MongoDB.

### Architecture
1.  **Ingest**: Loads `arxiv-metadata-oai-snapshot.json` into MongoDB.
2.  **Read & Partition**: Uses the **MongoDB Spark Connector (v10.x)** to read data into Spark.
    *   **Feature**: Uses `SamplePartitioner` to split data into **128MB chunks** for parallel processing.
3.  **Process**: Validates schema and partition distribution.

### Key Files (`mongo_etl/`)
*   `mongo_job.py`: Main ETL job using Spark 3.x and MongoDB Connector 10.5.0.
*   `load_sample_data.py`: Utility to load sample data into MongoDB via Spark.
*   `load_data_pymongo.py`: Lightweight utility to load data using PyMongo.

---

## ⚙️ Setup & Prerequisites

### Requirements
*   **Python**: 3.8+
*   **Java**: 8 or 11 (for Spark)
*   **Spark**: 3.x (Tested with 3.5.3)
*   **MongoDB**: Local instance running on port `27017`.
*   **MySQL**: (Optional, for Housing pipeline)

### Installation
1.  **Install Python Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Setup Hadoop/Winutils (Windows)**:
    *   Ensure `HADOOP_HOME` is set and `winutils.exe` is present in `%HADOOP_HOME%\bin`.

---

## ▶️ Execution Guide

### Running the MongoDB Arxiv Pipeline

**Step 1: Load Data into MongoDB**
Choose one method:
*   **Spark (Recommended)**: `python mongo_etl/load_sample_data.py`
*   **PyMongo (Fast)**: `python mongo_etl/load_data_pymongo.py`

**Step 2: Run the ETL Job**
```bash
python mongo_etl/mongo_job.py
```
*This will read from MongoDB `admin.test_data`, partition it, and display the schema and record count.*

**Step 3: Monitor with Spark History Server**
1.  **Set Log Directory** (PowerShell):
    ```powershell
    $env:SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/e:/Projects/etl_test_gemini_3/spark-events"
    ```
2.  **Start Server**:
    ```powershell
    spark-class.cmd org.apache.spark.deploy.history.HistoryServer
    ```
3.  **View UI**: Open `http://localhost:18080`

---

### Running the Housing Pipeline

**Manual Run**:
```bash
python spark_etl_job.py
```

**Testing**:
```bash
python test_etl_logic.py
```
