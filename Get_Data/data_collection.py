import pandas as pd
import logging
import os 
from datetime import datetime

batch_logger = logging.getLogger('batch_collection')
batch_logger.setLevel(logging.INFO)
batch_handler = logging.FileHandler('../logs/batch_collection.log')
batch_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
batch_handler.setFormatter(formatter)

# Add handler to logger
batch_logger.addHandler(batch_handler)

def collect_strava_data(start_week: int, end_week: int, specific_ids: list = None) -> pd.DataFrame:
    """Collect new Strava data for specified weeks and athletes
    
    Returns:
        pd.DataFrame: new_activities_df
    """
    from test_scraping import check_data_updates
    
    new_activities, _ = check_data_updates(
        start_week=start_week,
        end_week=end_week,
        specific_ids=specific_ids
    )
    
    return new_activities
def get_athletes_with_week45_only() -> list:
    """Get list of athlete IDs that only have week 45 scraped in 2024, split into groups of 10
    
    Returns:
        list: List of lists, each containing up to 10 athlete IDs
    """
    try:
        # Read metadata file
        metadata_df = pd.read_csv("../cleaned_athlete_metadata.csv")
        
        # Filter for athletes with exactly 1 week scraped (week 45)
        week45_athletes = metadata_df[metadata_df['2024 Weeks Scraped'] == 45]['Athlete ID'].tolist()
        week45_athletes.sort()
        
        # Split into groups of 10
        athlete_groups = [week45_athletes[i:i + 10] for i in range(0, len(week45_athletes), 10)]
        
        # Log batch information
        batch_logger.info(f"Found {len(week45_athletes)} athletes with only week 45 scraped")
        batch_logger.info(f"Split into {len(athlete_groups)} groups")
        
        # Log each group
        for i, group in enumerate(athlete_groups, 1):
            batch_logger.info(f"Group {i} ({len(group)} athletes): {sorted(group)}")
        
        # Print to console as well
        print(f"Found {len(week45_athletes)} athletes with only week 45 scraped")
        print(f"Athlete IDs: {sorted(week45_athletes)}")
        
        for i, group in enumerate(athlete_groups, 1):
            print(f"\nGroup {i} ({len(group)} athletes):")
            print(group)
        
        return athlete_groups
        
    except Exception as e:
        error_msg = f"Error getting week 45 athletes: {str(e)}"
        batch_logger.error(error_msg)
        print(error_msg)
        return []

if __name__ == "__main__":
    # Add a separator in log file for new runs
    batch_logger.info("=" * 80)
    batch_logger.info("Starting new batch collection run")
    
    athlete_groups = get_athletes_with_week45_only()
    if athlete_groups:
        batch_logger.info("\nCopy-paste format for each group:")
        for i, group in enumerate(athlete_groups, 1):
            batch_logger.info(f"Group {i} TARGET_IDS = {group}")