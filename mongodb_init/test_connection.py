from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    try:
        # Connect to MongoDB
        client = MongoClient(os.getenv('MONGO_URI'))
        
        # Test the connection
        db = client['elite_endurance']
        athlete_count = db.athlete_metadata.count_documents({})
        
        print(f"Successfully connected! Found {athlete_count} athletes.")
        
        # Test a sample query
        sample_athlete = db.athlete_metadata.find_one({})
        print("Sample athlete:", sample_athlete.get('Competitor'))
        
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()