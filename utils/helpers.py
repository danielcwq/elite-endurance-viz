from bson import ObjectId
import json
from datetime import datetime
import math

class MongoJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for MongoDB specific types"""
    def default(self, obj):
        try:
            if isinstance(obj, ObjectId):
                return str(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, float):
                if math.isnan(obj):
                    return None
                if math.isinf(obj):
                    return None
            return super().default(obj)
        except Exception as e:
            print(f"JSON Encoder Error: {str(e)}")
            raise

def mongo_to_json_serializable(data):
    """Convert MongoDB data to JSON-serializable format
    
    Args:
        data: MongoDB document or cursor result
        
    Returns:
        JSON serializable Python object
    """
    try:
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
        elif isinstance(data, float):
            if math.isnan(data):
                return None
            if math.isinf(data):
                return None
            return data
        return data
    except Exception as e:
        print(f"Serialization Error: {str(e)}")
        raise

def format_mongo_response(response, status_code=200):
    """Format MongoDB response with proper headers and status code
    
    Args:
        response: Data to be formatted
        status_code: HTTP status code (default: 200)
        
    Returns:
        Tuple of (formatted_response, status_code)
    """
    try:
        return {
            "data": mongo_to_json_serializable(response),
            "status": "success" if status_code == 200 else "error",
            "timestamp": datetime.utcnow().isoformat()
        }, status_code
    except Exception as e:
        print(f"Response Formatting Error: {str(e)}")
        return {"error": str(e)}, 500

def format_error_response(error_message: str, status_code: int = 500):
    """Format error response with consistent structure
    
    Args:
        error_message: Error message to be included
        status_code: HTTP status code (default: 500)
        
    Returns:
        Tuple of (error_response, status_code)
    """
    return {
        "error": error_message,
        "status": "error",
        "timestamp": datetime.utcnow().isoformat()
    }, status_code

def sanitize_mongo_data(data):
    """Sanitize MongoDB data by handling problematic values
    
    Args:
        data: Data to be sanitized
        
    Returns:
        Sanitized data safe for JSON serialization
    """
    try:
        if isinstance(data, dict):
            return {
                key: sanitize_mongo_data(value)
                for key, value in data.items()
                if value is not None  # Remove None values
            }
        elif isinstance(data, list):
            return [
                sanitize_mongo_data(item)
                for item in data
                if item is not None  # Remove None values
            ]
        elif isinstance(data, float):
            if math.isnan(data) or math.isinf(data):
                return None
            return data
        return data
    except Exception as e:
        print(f"Data Sanitization Error: {str(e)}")
        return None