from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['elite_endurance']  

activities = db['activities'].find({"Athlete Name": "Aaron Ahl"})
for activity in activities:
    print(activity)
