from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

def refresh_mongodb_data():
    """Refresh MongoDB collections with latest CSV data"""
    # Connect to MongoDB Atlas
    MONGO_URI = os.getenv("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client['elite_endurance']
    
    try:
        # Update athlete metadata
        athlete_metadata = pd.read_csv('../cleaned_athlete_metadata.csv')
        print(f"Read {len(athlete_metadata)} records from athlete metadata CSV")
        
        db['athlete_metadata'].drop()
        print("Dropped existing athlete metadata collection")
        
        athlete_metadata_dict = athlete_metadata.to_dict(orient='records')
        db['athlete_metadata'].insert_many(athlete_metadata_dict)
        
        metadata_count = db['athlete_metadata'].count_documents({})
        print(f"Successfully uploaded {metadata_count} records to athlete_metadata collection")
        
        # Update master IAAF database
        master_iaaf = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv')
        print(f"Read {len(master_iaaf)} records from master IAAF database CSV")
        
        db['master_iaaf'].drop()
        print("Dropped existing master IAAF collection")
        
        master_iaaf_dict = master_iaaf.to_dict(orient='records')
        db['master_iaaf'].insert_many(master_iaaf_dict)
        
        iaaf_count = db['master_iaaf'].count_documents({})
        print(f"Successfully uploaded {iaaf_count} records to master_iaaf collection")
        
    except Exception as e:
        print(f"Error refreshing MongoDB data: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    refresh_mongodb_data()