from bson import ObjectId
import json
from datetime import datetime

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def mongo_to_json_serializable(data):
    """Convert MongoDB data to JSON-serializable format"""
    if isinstance(data, list):
        return [mongo_to_json_serializable(item) for item in data]
    elif isinstance(data, dict):
        return {
            key: mongo_to_json_serializable(value)
            for key, value in data.items()
        }
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    return data