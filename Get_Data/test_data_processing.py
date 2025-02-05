import pandas as pd
from data_processing import process_data
import logging
from data_processing import calculate_athlete_metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configuration
    RAW_ACTIVITIES_PATH = "../data/tempdata/processed_AdamFOGG_to_ThomasBRIDGER_w45-47_20250204_125324.csv"
    START_WEEK = 45  # Update as needed
    END_WEEK = 47    # Update as needed
    TARGET_IDs = [4814818,4928335,45537525]
    

    try:
        master_df = pd.read_csv("../data/metadata/master_iaaf_database_with_strava.csv")
        # Process the data
        success = process_data(
            start_week=START_WEEK,
            end_week=END_WEEK,
            raw_activities_path=RAW_ACTIVITIES_PATH,
            target_ids=TARGET_IDs
        )
        
        if success:
            logger.info("Data processing completed successfully")
        else:
            logger.error("Data processing failed")
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")

def test_specific_athlete_metrics():
    """Test metrics calculation for athlete ID 4928335"""
    try:
        # Read activities database
        activities_df = pd.read_csv("../indiv_activities_full.csv")
        
        # Calculate metrics for just this athlete
        target_id = [4928335]  # List with single ID
        
        print("\nTesting metrics for athlete ID 4928335:")
        print("-" * 50)
        
        # Show raw activities data
        athlete_activities = activities_df[activities_df['Athlete ID'] == target_id[0]]
        print(f"\nTotal activities found: {len(athlete_activities)}")
        print("\nActivity Summary:")
        print(athlete_activities.groupby('Type').agg({
            'Distance (km)': ['count', 'sum'],
            'Time (min)': 'sum'
        }))
        
        # Calculate metrics using existing function
        metrics = calculate_athlete_metrics(target_id)
        
        if not metrics.empty:
            print("\nCalculated Metrics:")
            print("-" * 50)
            for col in metrics.columns:
                if col != 'Athlete ID':
                    print(f"{col:25}: {metrics.iloc[0][col]}")
        else:
            print("No metrics calculated")
            
    except Exception as e:
        print(f"Error in test_specific_athlete_metrics: {str(e)}")

if __name__ == "__main__":
    test_specific_athlete_metrics()

#if __name__ == "__main__":
#    main()