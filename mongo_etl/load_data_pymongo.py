"""
Load sample data into MongoDB using pymongo
This avoids Spark connector compatibility issues
"""
from pymongo import MongoClient
import json

def load_sample_data_pymongo():
    """Load sample arxiv data into MongoDB using pymongo"""
    
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client['admin']
    collection = db['test_data']
    
    print(">>> Connected to MongoDB")
    print(">>> Database: admin")
    print(">>> Collection: test_data")
    
    # Clear existing data
    collection.delete_many({})
    print(">>> Cleared existing data")
    
    # Read and load JSON data (limit to 10,000 records)
    json_file = "arxiv-metadata-oai-snapshot.json"
    print(f">>> Reading {json_file} (limited to 10,000 records)...")
    
    documents = []
    with open(json_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 10000:
                break
            try:
                doc = json.loads(line)
                documents.append(doc)
            except json.JSONDecodeError:
                continue
    
    print(f">>> Loaded {len(documents)} documents from JSON")
    
    # Insert into MongoDB
    if documents:
        result = collection.insert_many(documents)
        print(f">>> ✓ Inserted {len(result.inserted_ids)} documents into MongoDB")
    
    # Verify
    count = collection.count_documents({})
    print(f">>> Total documents in collection: {count}")
    
    # Show sample document
    sample = collection.find_one()
    if sample:
        print(f"\n>>> Sample document fields:")
        for key in list(sample.keys())[:10]:
            print(f"  - {key}")
    
    client.close()
    print("\n>>> ✓ Data loading complete!")

if __name__ == "__main__":
    load_sample_data_pymongo()
