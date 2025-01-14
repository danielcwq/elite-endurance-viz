from fasthtml.common import *
from database.connection import DatabaseConnection
from urllib.parse import unquote
from utils.helpers import mongo_to_json_serializable
import traceback

def setup_routes(rt):
    db = DatabaseConnection.get_instance()
    
    @rt("/api/athlete/{name}")
    def get(name: str):
        try:
            print(f"API Request received for athlete: {name}")
            decoded_name = unquote(name)
            print(f"Decoded name: {decoded_name}")
            
            print("Attempting database connection...")
            athlete = db.get_athlete_metadata(decoded_name)
            print(f"DB Response: {athlete is not None}")
            
            if not athlete:
                print(f"Athlete not found: {decoded_name}")
                return {"error": "Athlete not found"}, 404
                
            activities = db.get_athlete_activities(decoded_name)
            return {
                "athlete": mongo_to_json_serializable(athlete),
                "activities": mongo_to_json_serializable(activities)
            }
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"API Error: {str(e)}\nTraceback: {error_trace}")
            return {"error": str(e)}, 500
    
    @rt("/api/activities/{name}")
    def get(name: str):
        try:
            decoded_name = unquote(name)
            activities = db.get_athlete_activities(decoded_name)
            return {
                "activities": mongo_to_json_serializable(activities)
            }
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"API Error: {str(e)}\nTraceback: {error_trace}")
            return {"error": str(e)}, 500
            
    @rt("/api/test")
    def get():
        """Test endpoint to verify MongoDB connection"""
        try:
            count = db.db.athlete_metadata.count_documents({})
            sample = db.db.athlete_metadata.find_one({})
            return {
                "status": "connected",
                "athlete_count": count,
                "database": "elite_endurance",
                "sample_athlete": mongo_to_json_serializable(sample)
            }
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"Test API Error: {str(e)}\nTraceback: {error_trace}")
            return {"error": str(e)}, 500