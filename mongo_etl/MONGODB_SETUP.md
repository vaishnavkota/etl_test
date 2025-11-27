# MongoDB Setup Instructions

## Option 1: Local MongoDB (Recommended for Testing)

### Installation
1. Download MongoDB Community Server from: https://www.mongodb.com/try/download/community
2. Install with default settings
3. MongoDB will run on `localhost:27017` by default

### Start MongoDB
```powershell
# Start MongoDB service
net start MongoDB
```

### Update mongo_job.py
Change the MongoDB configuration to:
```python
mongodb_conf = {
    "source": "mongodb",
    "uri": "mongodb://localhost:27017",
    "database": "test",
    "collection": "arxiv"
}
```

### Load Sample Data
You can load data from the JSON file first:
```python
# In mongo_job.py, add this before reading from MongoDB:
df_json = spark.read.json("arxiv-metadata-oai-snapshot.json").limit(1000)
df_json.write.format("mongo") \
    .option("uri", "mongodb://localhost:27017/test.arxiv") \
    .mode("overwrite") \
    .save()
```

## Option 2: MongoDB Atlas Free Tier

### Setup Steps
1. Go to https://www.mongodb.com/cloud/atlas/register
2. Create a free account
3. Create a free M0 cluster (512MB storage)
4. Load sample datasets (optional)
5. Create a database user with username/password
6. Add your IP address to the IP Access List (or use `0.0.0.0/0` for testing)
7. Get your connection string from the "Connect" button

### Connection String Format
```
mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/<database>?retryWrites=true&w=majority
```

### Update mongo_job.py
```python
mongodb_conf = {
    "source": "mongodb",
    "uri": "mongodb+srv://your-username:your-password@your-cluster.mongodb.net",
    "database": "your-database",
    "collection": "your-collection"
}
```

## Current Status

**✓ JSON File Reading**: Works perfectly (2.8M records tested)
**✗ MongoDB Atlas**: No public test cluster available
**? Local MongoDB**: Requires installation

## Recommendation

For immediate testing, use **Local MongoDB** or keep using the **JSON file source** which is already working.
