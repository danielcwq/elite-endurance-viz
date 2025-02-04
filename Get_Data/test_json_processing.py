import pandas as pd
import json
import logging
from strava_scrape_new import process_activities

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_json_processing(raw_json_path: str):
    """Test JSON processing on existing raw JSON data file"""
    logger.info(f"Testing JSON processing on file: {raw_json_path}")
    
    try:
        # Read the CSV file
        df_json = pd.read_csv(raw_json_path)
        logger.info(f"Loaded data with {len(df_json)} entries")
        
        # Print data structure for debugging
        print("\nDataFrame Structure:")
        print(df_json.columns.tolist())
        print(f"\nNumber of unique athletes: {df_json['Name'].nunique()}")
        print("Athletes:", df_json['Name'].unique())
        
        # Process each athlete's data
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
                    # Print problematic JSON snippet
                    print(f"\nProblematic JSON (first 200 chars):\n{row['JSON Data'][:200]}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing activities for {athlete_name}: {str(e)}")
                    continue
        
        if all_processed_activities:
            final_df = pd.concat(all_processed_activities, ignore_index=True)
            logger.info(f"Successfully processed {len(final_df)} total activities")
            
            # Save processed results
            output_path = raw_json_path.replace('raw_json', 'processed_test')
            final_df.to_csv(output_path, index=False)
            logger.info(f"Saved processed activities to: {output_path}")
            
            # Print summary
            print("\nProcessed Activities Summary:")
            print(f"Total activities: {len(final_df)}")
            print("\nActivities by athlete:")
            print(final_df.groupby('Athlete Name').size())
            print("\nSample of processed activities:")
            print(final_df.head())
            
            return final_df
        else:
            logger.warning("No activities were processed successfully")
            return None
            
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        return None

if __name__ == "__main__":
    # Point to the specific file
    raw_json_path = '../data/tempdata/raw_json_AdamFOGG_to_ThomasBRIDGER_w45-47_20250203_202647.csv'
    test_json_processing(raw_json_path)