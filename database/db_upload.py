from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# Connect to MongoDB Atlas
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['elite_endurance']  # Database name

# Test connection
print("Databases in the cluster:", client.list_database_names())

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(ROOT_DIR, '..')  # Navigate to the project root
metadata_csv_path = os.path.join(CSV_DIR, 'cleaned_athlete_metadata.csv')
activities_csv_path = os.path.join(CSV_DIR, 'indiv_activities_full.csv')

athlete_metadata = pd.read_csv(metadata_csv_path)
activities = pd.read_csv(activities_csv_path)

# Insert athlete metadata into 'athlete_metadata' collection
athlete_metadata_dict = athlete_metadata.to_dict(orient='records')
db['athlete_metadata'].insert_many(athlete_metadata_dict)

# Insert activities into 'activities' collection
activities_dict = activities.to_dict(orient='records')
db['activities'].insert_many(activities_dict)

print("Data uploaded successfully!")
