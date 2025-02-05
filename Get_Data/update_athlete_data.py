import logging
from datetime import datetime
import os
import sys
from data_processing import process_data
from data_collection import collect_strava_data

# Setup logging
logging.basicConfig(
    filename='../logs/automation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_data_pipeline(start_week: int = 45, end_week: int = 47, target_ids: list = None):
    """Run complete data pipeline from scraping to MongoDB sync
    
    Args:
        start_week: Starting week number
        end_week: Ending week number
        target_ids: List of specific athlete IDs to process
    """
    logger.info(f"Starting data pipeline for weeks {start_week}-{end_week}")
    logger.info(f"Processing athletes: {target_ids}")
    
    try:
        # Step 1: Collect new Strava data
        new_activities = collect_strava_data(
            start_week=start_week,
            end_week=end_week,
            specific_ids=target_ids
        )
        
        if new_activities is None:
            logger.error("Failed to collect new data")
            return False
            
        # Save new activities to temp file
        temp_file = f"../data/tempdata/processed_activities_w{start_week}-{end_week}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        new_activities.to_csv(temp_file, index=False)
        logger.info(f"Saved new activities to {temp_file}")
            
        # Step 2: Process and update databases
        success = process_data(
            start_week=start_week,
            end_week=end_week,
            raw_activities_path=temp_file,
            target_ids=target_ids
        )
        
        if success:
            # Step 3: Sync with MongoDB
            from ..mongodb_init.db_update import refresh_mongodb_data
            refresh_mongodb_data()
            logger.info("Complete pipeline executed successfully")
            return True
            
        logger.error("Data processing failed")
        return False
            
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        return False

if __name__ == "__main__":
    # Example usage
    TARGET_IDS = [4814818, 4928335, 45537525]  # Example athlete IDs
    run_data_pipeline(
        start_week=45, 
        end_week=52, 
        target_ids=TARGET_IDS
    )