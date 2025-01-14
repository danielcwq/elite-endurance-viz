import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"