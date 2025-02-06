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
            logger.info("Complete pipeline executed successfully without uploading to db")
            return True
            
        logger.error("Data processing failed")
        return False
            
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        return False

if __name__ == "__main__":
    # Example usage
    TARGET_IDS = [7677443, 7774046, 7814602, 8112897, 8140136, 8483601, 8728583, 8820965, 8841155, 9351341, 9389758, 9559332, 9588583, 9996058, 10175779, 10206606, 10318023, 10444014, 10513706, 10609301, 10799453, 10816005, 10984855, 11157470, 11328161, 11747326, 11799655, 12012236, 12019441, 12527724]
    run_data_pipeline(
        start_week=45, 
        end_week=52, 
        target_ids=TARGET_IDS
    )