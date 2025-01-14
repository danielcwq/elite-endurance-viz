from fasthtml.common import *
from database.connection import DatabaseConnection
from urllib.parse import unquote

def setup_routes(rt):
    db = DatabaseConnection.get_instance()
    
    @rt("/api/athlete/{name}")
    def get(name: str):
        decoded_name = unquote(name)
        athlete = db.get_athlete_metadata(decoded_name)
        
        if not athlete:
            return {"error": "Athlete not found"}, 404
            
        activities = db.get_athlete_activities(decoded_name)
        return {
            "athlete": athlete,
            "activities": activities
        }
    
    @rt("/api/activities/{name}")
    def get(name: str):
        decoded_name = unquote(name)
        activities = db.get_athlete_activities(decoded_name)
        return {"activities": activities}