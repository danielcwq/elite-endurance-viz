import pandas as pd
import logging
from strava_scrape_new import consolidate_weekly_data, web_driver, login_strava, setup_logging, process_activities
import json
import os 

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)
TEMP_DATA_DIR = '../data/metadata'
def run_test():
    """Run test scraping for a small subset of athletes"""
    logger.info("Starting test scraping")
    
    # Initialize driver
    driver = web_driver()
    
    try:
        # Login to Strava
        if not login_strava(driver):
            logger.error("Failed to login to Strava")
            return
        
        logger.info("Successfully logged in to Strava")
        
        # Read master database
        try:
            df = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv')
            logger.info(f"Loaded master database with {len(df)} entries")
            
            # Filter for specific test athletes
            specific_ids = [45537525, 4814818, 4928335]
            test_df = df[df['Athlete ID'].isin(specific_ids)]
            logger.info(f"Selected {len(test_df)} athletes for testing")
            
            # Use existing consolidate_weekly_data function
            json_df = consolidate_weekly_data(
                driver=driver,
                df=test_df,
                start_week=3,
                end_week=7
            )
            
            if not json_df.empty:
                # Process each athlete's activities
                all_processed_activities = []
                for athlete_name in json_df['Name'].unique():
                    athlete_json_data = json_df[json_df['Name'] == athlete_name]['JSON Data'].iloc[0]
                    try:
                        processed_df = process_activities(
                            json.loads(athlete_json_data),
                            athlete_name
                        )
                        if not processed_df.empty:
                            all_processed_activities.append(processed_df)
                    except Exception as e:
                        logger.error(f"Error processing activities for {athlete_name}: {str(e)}")
                        continue
                
                if all_processed_activities:
                    final_df = pd.concat(all_processed_activities, ignore_index=True)
                    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                    output_file = os.path.join(TEMP_DATA_DIR, f'test_results_{timestamp}.csv')
                    final_df.to_csv(output_file, index=False)
                    logger.info(f"Saved test results to {output_file}")
                else:
                    logger.warning("No activities were processed successfully")
            
        except Exception as e:
            logger.error(f"Error during data collection: {str(e)}")
            
    finally:
        driver.quit()
        logger.info("Test completed, browser closed")

if __name__ == "__main__":
    run_test()