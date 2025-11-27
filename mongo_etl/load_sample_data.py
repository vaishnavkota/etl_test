"""
Load sample data from arxiv JSON into MongoDB
This creates test data for the MongoDB ETL job
"""
from pyspark.sql import SparkSession
import os

def load_sample_data():
    """Load sample arxiv data into MongoDB"""
    
    # Set local Hadoop home
    cwd = os.getcwd()
    hadoop_home = os.path.join(cwd, 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
    
    # Initialize Spark with MongoDB connector
    spark = SparkSession.builder \
        .appName("LoadMongoData") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .getOrCreate()
    
    print(">>> Spark Session Created")
    
    try:
        # Read sample from JSON file (limit to 10,000 records for testing)
        json_file = "arxiv-metadata-oai-snapshot.json"
        print(f">>> Reading {json_file} (limited to 10,000 records)...")
        df = spark.read.json(json_file).limit(10000)
        
        print(f">>> Loaded {df.count()} records")
        print(">>> Schema:")
        df.printSchema()
        
        # Write to MongoDB
        mongo_uri = "mongodb://localhost:27017/admin.test_data"
        print(f">>> Writing to MongoDB: {mongo_uri}")
        
        df.write \
            .format("mongodb") \
            .option("uri", mongo_uri) \
            .mode("overwrite") \
            .save()
        
        print(">>> ✓ Data successfully written to MongoDB!")
        print(">>> Database: admin")
        print(">>> Collection: arxiv")
        print(f">>> Records: {df.count()}")
        
    except Exception as e:
        print(f">>> ✗ Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    load_sample_data()
