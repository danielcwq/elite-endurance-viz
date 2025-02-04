import pandas as pd
import shutil
import logging
from strava_scrape_new import consolidate_weekly_data, web_driver, login_strava, setup_logging, process_activities
import json
import os 
from datetime import datetime

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)
TEMP_DATA_DIR = '../data/tempdata'
START_WEEK = 50 
END_WEEK = 52

def generate_filename(df, file_type, start_week, end_week):
    """Generate informative filename for temporary data files
    
    Args:
        df (pd.DataFrame): DataFrame containing athlete data
        file_type (str): Type of data being saved ('raw' or 'processed')
        start_week (int): Starting week number
        end_week (int): Ending week number
    """
    # Get first and last athlete names
    first_athlete = df['Name'].iloc[0] if not df.empty else 'unknown'
    last_athlete = df['Name'].iloc[-1] if not df.empty else 'unknown'
    
    # Clean names for filename (remove spaces and special characters)
    first_athlete = ''.join(e for e in first_athlete if e.isalnum())
    last_athlete = ''.join(e for e in last_athlete if e.isalnum())
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create filename
    return f"{file_type}_{first_athlete}_to_{last_athlete}_w{start_week}-{end_week}_{timestamp}.csv"

def run_test():
    """Run test scraping for a small subset of athletes"""
    logger.info("Starting test scraping")
    
    # Initialize driver
    driver = web_driver()
    start_week = 45
    end_week = 52

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
            df_weekly, df_json = consolidate_weekly_data(
                driver=driver,
                df=test_df,
                start_week=start_week,
                end_week=end_week
            )
            
            if not df_weekly.empty and not df_json.empty:
                # Save metadata
                metadata_filename = generate_filename(df_weekly, 'metadata', start_week, end_week)
                metadata_filepath = os.path.join(TEMP_DATA_DIR, metadata_filename)
                df_weekly.to_csv(metadata_filepath, index=False)
                logger.info(f"Saved weekly metadata to {metadata_filepath}")
                
                # Save raw JSON data
                json_filename = generate_filename(df_json, 'raw_json', start_week, end_week)
                json_filepath = os.path.join(TEMP_DATA_DIR, json_filename)
                df_json.to_csv(json_filepath, index=False)
                logger.info(f"Saved raw JSON data to {json_filepath}")
                
                # Process each athlete's activities
                all_processed_activities = []
                for athlete_name in df_json['Name'].unique():
                    athlete_data = df_json[df_json['Name'] == athlete_name]
                    logger.info(f"\nProcessing data for {athlete_name}")
                    
                    for _, row in athlete_data.iterrows():
                        try:
                            # Load JSON data
                            json_data = json.loads(row['JSON Data'])
                            
                            # Process activities for this athlete
                            processed_df = process_activities(json_data, athlete_name)
                            
                            if not processed_df.empty:
                                all_processed_activities.append(processed_df)
                                logger.info(f"Successfully processed activities for {athlete_name}")
                            else:
                                logger.warning(f"No activities found for {athlete_name}")
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error for {athlete_name}: {str(e)}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing activities for {athlete_name}: {str(e)}")
                            continue
                
                if all_processed_activities:
                    final_df = pd.concat(all_processed_activities, ignore_index=True)
                    processed_filename = generate_filename(df_json, 'processed', start_week, end_week)
                    processed_filepath = os.path.join(TEMP_DATA_DIR, processed_filename)
                    final_df.to_csv(processed_filepath, index=False)
                    logger.info(f"Saved processed activities to {processed_filepath}")
                else:
                    logger.warning("No activities were processed successfully")
            else:
                logger.warning("No data was collected from Strava")
            
        except Exception as e:
            logger.error(f"Error during data collection: {str(e)}")
            
    finally:
        driver.quit()
        logger.info("Test completed, browser closed")

def check_data_updates(start_week: int = 45, end_week: int = 47, specific_ids: list = [45537525, 4814818, 4928335]):
    """Compare new data with existing databases and return changes"""
    logger.info(f"Starting data comparison for weeks {start_week}-{end_week}")
    
    # Create backups before any changes
    backup_dir = '../data/metadata/backup'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for file in ['master_iaaf_database_with_strava.csv', 'cleaned_athlete_metadata.csv', 'indiv_activities_full.csv']:
        src_path = os.path.join('..', file)
        if os.path.exists(src_path):
            backup_path = os.path.join(backup_dir, f"{timestamp}_{file}")
            shutil.copy2(src_path, backup_path)
            logger.info(f"Created backup: {backup_path}")
    
    # Initialize driver
    driver = web_driver()
    
    try:
        # Login to Strava
        if not login_strava(driver):
            logger.error("Failed to login to Strava")
            return None, None
            
        # Load existing databases
        master_df = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv')
        activities_df = pd.read_csv('../indiv_activities_full.csv')
        
        # Get new data
        test_df = master_df[master_df['Athlete ID'].isin(specific_ids)].copy()
        df_weekly, df_json = consolidate_weekly_data(
            driver=driver,
            df=test_df,
            start_week=start_week,
            end_week=end_week
        )
        
        if not df_weekly.empty and not df_json.empty:
            # Save metadata to tempdata
            metadata_filename = generate_filename(df_weekly, 'metadata', start_week, end_week)
            metadata_filepath = os.path.join(TEMP_DATA_DIR, metadata_filename)
            df_weekly.to_csv(metadata_filepath, index=False)
            logger.info(f"Saved weekly metadata to {metadata_filepath}")
            
            # Save raw JSON data to tempdata
            json_filename = generate_filename(df_json, 'raw_json', start_week, end_week)
            json_filepath = os.path.join(TEMP_DATA_DIR, json_filename)
            df_json.to_csv(json_filepath, index=False)
            logger.info(f"Saved raw JSON data to {json_filepath}")
        
            # Process new activities
            all_processed_activities = []
            for athlete_name in df_json['Name'].unique():
                athlete_data = df_json[df_json['Name'] == athlete_name]
                logger.info(f"\nProcessing data for {athlete_name}")
                for _, row in athlete_data.iterrows():
                    try:
                        # Load JSON data
                        json_data = json.loads(row['JSON Data'])
                        
                        # Process activities for this athlete
                        processed_df = process_activities(json_data, athlete_name)
                        
                        if not processed_df.empty:
                            all_processed_activities.append(processed_df)
                            logger.info(f"Successfully processed activities for {athlete_name}")
                        else:
                            logger.warning(f"No activities found for {athlete_name}")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error for {athlete_name}: {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing activities for {athlete_name}: {str(e)}")
                        continue
                    
            if all_processed_activities:
                new_activities_df = pd.concat(all_processed_activities, ignore_index=True)
                
                # Save processed activities to tempdata
                processed_filename = generate_filename(df_json, 'processed', start_week, end_week)
                processed_filepath = os.path.join(TEMP_DATA_DIR, processed_filename)
                new_activities_df.to_csv(processed_filepath, index=False)
                logger.info(f"Saved processed activities to {processed_filepath}")
                
                # Get new activities
                new_activities = new_activities_df[~new_activities_df['Activity ID'].isin(activities_df['Activity ID'])]
                logger.info(f"Found {len(new_activities)} new activities")
                
                # Update master database weeks scraped
                updated_master = test_df.copy()
                updated_master.loc[:, '2024_Weeks_Scraped'] = int(end_week)
                
                return new_activities, updated_master
            else:
                logger.warning("No activities were processed successfully")
                return None, None
        else:
            logger.warning("No data was collected from Strava")
            return None, None
            
    except Exception as e:
        logger.error(f"Error during data collection: {str(e)}")
        return None, None
    finally:
        driver.quit()
        logger.info("Test completed, browser closed")

if __name__ == "__main__":
    new_activities, updated_master = check_data_updates()
    if new_activities is not None:
        print("\nNew activities that would be added to indiv_activities_full.csv:")
        print(f"Number of new activities: {len(new_activities)}")
        print(new_activities)
        
        print("\nUpdated rows in master_iaaf_database_with_strava.csv:")
        print(updated_master[['Athlete ID', 'Competitor', '2024_Weeks_Scraped']])
