from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd
import logging
from datetime import datetime

load_dotenv()

logging.basicConfig(
    filename='../logs/automation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def refresh_mongodb_data():
    """Refresh MongoDB collections with latest CSV data"""
    # Connect to MongoDB Atlas
    MONGO_URI = os.getenv("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client['elite_endurance']
    
    try:
        # Update athlete metadata
        athlete_metadata = pd.read_csv('../cleaned_athlete_metadata.csv')
        db['athlete_metadata'].drop()
        db['athlete_metadata'].insert_many(athlete_metadata.to_dict(orient='records'))
        logger.info(f"Uploaded {len(athlete_metadata)} records to athlete_metadata collection")
        
        # Update master IAAF database
        master_iaaf = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv')
        db['master_iaaf'].drop()
        db['master_iaaf'].insert_many(master_iaaf.to_dict(orient='records'))
        logger.info(f"Uploaded {len(master_iaaf)} records to master_iaaf collection")
        
        # Update individual activities
        activities = pd.read_csv('../indiv_activities_full.csv')
        db['activities'].drop()
        db['activities'].insert_many(activities.to_dict(orient='records'))
        logger.info(f"Uploaded {len(activities)} records to activities collection")

        log_entry = {
            'timestamp': datetime.now(),
            'athlete_metadata_count': len(athlete_metadata),
            'master_iaaf_count': len(master_iaaf),
            'activities_count': len(activities)
        }
        db['update_logs'].insert_one(log_entry)
        
        return True
        
    except Exception as e:
        logger.error(f"Error refreshing MongoDB data: {str(e)}")
        return False
    finally:
        client.close()

if __name__ == "__main__":
    refresh_mongodb_data()