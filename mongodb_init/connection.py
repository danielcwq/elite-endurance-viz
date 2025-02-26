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
        # Make case-insensitive search
        athlete_name = athlete_name.strip()
        result = self.db.athlete_metadata.find_one({
            "Competitor": {"$regex": f"^{athlete_name}$", "$options": "i"}
        })
        return result

    def get_athlete_activities(self, athlete_name: str):
        # Make case-insensitive search
        athlete_name = athlete_name.strip()
        return list(self.db.activities.find({
            "Athlete Name": {"$regex": f"^{athlete_name}$", "$options": "i"}
        }).sort("Start Date", -1))
    
