from pymongo import MongoClient
from config.settings import MONGO_URI

class DatabaseConnection:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client['elite_endurance']
    
    def get_athlete_metadata(self, athlete_name: str):
        return self.db.athlete_metadata.find_one({"Competitor": athlete_name})
    
    def get_athlete_activities(self, athlete_name: str):
        return list(self.db.activities.find(
            {"Athlete Name": athlete_name}
        ).sort("Start Date", -1))
    
