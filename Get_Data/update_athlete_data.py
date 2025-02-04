import logging
from datetime import datetime
import os
import sys
from data_processing import process_data

# Add mongodb_init to path for importing
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from mongodb_init.db_update import refresh_mongodb_data

# Setup logging
logging.basicConfig(
    filename='../logs/automation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_data_pipeline(start_week: int = 45, end_week: int = 47):
    """Run complete data pipeline from scraping to MongoDB sync"""
    logger.info(f"Starting data pipeline for weeks {start_week}-{end_week}")
    
    try:
        # Step 1: Process new Strava data and update CSVs
        if process_data(start_week, end_week):
            logger.info("Data processing completed successfully")
            
            # Step 2: Sync with MongoDB
            # Note: Need to modify refresh_mongodb_data() to handle indiv_activities_full
            refresh_mongodb_data()
            logger.info("MongoDB sync completed")
            return True
        else:
            logger.error("Data processing failed")
            return False
            
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        return False

if __name__ == "__main__":
    run_data_pipeline()