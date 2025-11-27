# Example configurations for MongoSpark.load()

# 1. Read from MongoDB (using MongoDB Atlas free tier or local MongoDB)
mongodb_conf = {
    "source": "mongodb",
    "uri": "mongodb+srv://readonly:readonly@cluster0.example.mongodb.net",  # Example MongoDB Atlas URI
    "database": "sample_mflix",  # Example database
    "collection": "movies"  # Example collection
}

# 2. Read from local JSON file with sample partitioner
json_conf = {
    "source": "json",
    "file_path": "arxiv-metadata-oai-snapshot.json",
    "num_partitions": 80,
    "partitioner": "sample",
    "sample_fraction": 0.1
}

# Usage:
# df = MongoSpark.load(spark, conf=mongodb_conf)  # For MongoDB
# df = MongoSpark.load(spark, conf=json_conf)     # For JSON file
