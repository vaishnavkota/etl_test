from pyspark.sql import SparkSession
import os

# Helper class to match user's requested syntax
class MongoSpark:
    @staticmethod
    def load(spark, conf=None):
        """
        Loads data from MongoDB or a local JSON file using the Spark session and configuration.
        
        Args:
            spark (SparkSession): The Spark session.
            conf (dict): Configuration options including:
                - source: "mongodb" or "json" (default: "json")
                - uri: MongoDB connection URI (for MongoDB source)
                - database: MongoDB database name (for MongoDB source)
                - collection: MongoDB collection name (for MongoDB source)
                - file_path: Path to the JSON file (for JSON source)
                - num_partitions: Number of partitions (default: 80 for ~64MB partitions)
                - partitioner: Partitioning strategy ("sample" or "default")
                - sample_fraction: Fraction of data to sample (default: 0.1 for sample partitioner)
        
        Returns:
            DataFrame: The loaded DataFrame.
        """
        if conf is None:
            conf = {}
        
        source = conf.get("source", "json")
        
        if source == "mongodb":
            # Read from MongoDB
            uri = conf.get("uri","test") #"mongodb://localhost:27017")
            database = conf.get("database", "admin")
            collection = conf.get("collection", "test_data")
            
            # Build MongoDB URI with database and collection
            full_uri = f"{uri}/{database}.{collection}"
            
            df = spark.read \
                .format("mongodb") \
                .option("uri", full_uri) \
                .load()
            
            print(f">>> Loaded from MongoDB: {full_uri}")
            
        else:
            # Read from JSON file
            file_path = conf.get("file_path", "arxiv-metadata-oai-snapshot.json")
            num_partitions = conf.get("num_partitions", 80)
            partitioner = conf.get("partitioner", "default")
            sample_fraction = conf.get("sample_fraction", 0.1)
            
            # Read JSON file
            df = spark.read.json(file_path)
            
            # Apply partitioning strategy
            if partitioner == "sample":
                # Sample-based partitioning: sample data first, then repartition
                df_sampled = df.sample(withReplacement=False, fraction=sample_fraction)
                df = df.repartition(num_partitions)
                print(f">>> Using sample partitioner with {sample_fraction*100}% sample fraction")
            else:
                # Default repartitioning
                df = df.repartition(num_partitions)
            
            print(f">>> Loaded from JSON file: {file_path}")
        
        return df

def main():
    # Set SPARK_HOME to the virtual environment's PySpark installation
    # This overrides any system-wide SPARK_HOME (like Spark 4.0.1) that causes version mismatch
    import pyspark
    os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)

    # Set local Hadoop home to avoid system-wide configuration
    cwd = os.getcwd()
    hadoop_home = os.path.join(cwd, 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
    
    # Initialize Spark Session with MongoDB Connector
    # Supports both MongoDB and local JSON file reading
    spark_events_dir = os.path.join(cwd, "spark-events").replace("\\", "/")
    spark = SparkSession.builder \
        .appName("MongoETLJob") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.rolling.enabled", "false") \
        .config("spark.eventLog.dir", f"file:///{spark_events_dir}") \
        .getOrCreate()

    print(">>> Spark Session Created")

    # JSON File Path (for fallback)
    json_file_path = "arxiv-metadata-oai-snapshot.json"
    
    try:
        # Option 1: Read from MongoDB (local MongoDB with admin database)
        print(">>> Reading from local MongoDB (admin.test_data)...")
        df = spark.read.format("mongodb") \
            .option("connection.uri", "mongodb://localhost:27017") \
            .option("database", "admin") \
            .option("collection", "test_data") \
            .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner") \
            .option("partitioner.options.partition.size", "128") \
            .option("partitioner.options.samples.per.partition", "10") \
            .load()

        print(f"Number of Partitions: {df.rdd.getNumPartitions()}")
        
        # Option 2: Read from local JSON file using MongoSpark.load()
        # Uncomment the following to read from JSON instead of MongoDB
        """
        print(f">>> Reading JSON file: {json_file_path} using MongoSpark.load()...")
        
        """
        
        print(f">>> Data loaded successfully")
        print(f">>> Number of partitions: {df.rdd.getNumPartitions()}")
        print(">>> Schema:")
        df.printSchema()
        print(">>> Sample data (first 5 rows):")
        df.show(100,truncate=True)
        
        # Show partition distribution
        count = df.count()
        print(f">>> Total records: {count}")
        
        # Estimate in-memory size (rough approximation based on schema and count)
        # This is just a heuristic; Spark History Server shows the exact "Input Size" for the stage.
        estimated_size_mb = (count * 1500) / (1024 * 1024) # Assuming ~1.5KB per row based on previous stats
        print(f">>> Estimated In-Memory Size: ~{estimated_size_mb:.2f} MB")

        df.write.format("noop").mode("overwrite").save()
    except Exception as e:
        print(f"Error during ETL process: {e}")


if __name__ == "__main__":
    main()
