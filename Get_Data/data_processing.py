import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from test_scraping import check_data_updates

# Setup logging
logging.basicConfig(
    filename='../logs/data_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def time_to_minutes(time_str):
    """Convert time string to minutes"""
    try:
        hours, minutes, seconds = 0, 0, 0
        
        # Parse "1h 14m" format
        if "h" in time_str:
            parts = time_str.split("h")
            hours = int(parts[0].strip())
            time_str = parts[1].strip()
        
        # Parse "50m 1s" or "0m XXs"
        if "m" in time_str:
            parts = time_str.split("m")
            minutes = int(parts[0].strip())
            if "s" in parts[1]:
                seconds = int(parts[1].replace("s", "").strip())
        elif "s" in time_str:
            # Handle "0m XXs" or "XXs"
            seconds = int(time_str.replace("s", "").strip())

        # Convert to decimal minutes
        return round(hours * 60 + minutes + seconds / 60, 2)
    except:
        return 0  # Default for invalid entries


def clean_activities_data(df: pd.DataFrame, target_athletes: list) -> pd.DataFrame:
    """Clean and convert activity data types"""
    logger.info("Cleaning activity data...")
    
    # Read existing database to get last serial number and validate structure
    try:
        existing_df = pd.read_csv("../indiv_activities_full.csv")
        last_serial = existing_df.iloc[-1, 0] if not existing_df.empty else 0
        expected_columns = existing_df.columns
    except Exception as e:
        logger.error(f"Error reading existing database: {str(e)}")
        return pd.DataFrame()

    # Make a copy to avoid modifying original
    clean_df = df.copy()
    
    # Clean athlete names
    clean_df['Athlete Name'] = clean_df['Athlete Name'].str.strip().str.title()
    
    # Convert Time to minutes if needed
    if 'Time (min)' not in clean_df.columns and 'Time' in clean_df.columns:
        logger.info("Converting Time to minutes...")
        clean_df['Time (min)'] = clean_df['Time'].apply(time_to_minutes)
        # Calculate Activity Time (seconds)
        clean_df['Activity Time (s)'] = clean_df['Time (min)'] * 60
    
    # Convert Distance to float and ensure it's in km
    clean_df['Distance (km)'] = pd.to_numeric(clean_df['Distance (km)'], errors='coerce').fillna(0).astype(float)
    
    # Calculate Pace (min/km)
    clean_df['Pace (min/km)'] = clean_df['Time (min)'] / clean_df['Distance (km)']
    clean_df['Pace (min/km)'] = clean_df['Pace (min/km)'].replace([np.inf, -np.inf], np.nan)
    
    # Add null column for Pace (min/mi)
    clean_df['Pace (min/mi)'] = np.nan
    
    # Add serial numbers
    num_new_rows = len(clean_df)
    clean_df.insert(0, 'Serial', range(last_serial + 1, last_serial + num_new_rows + 1))
    
    # Ensure correct data types for all columns
    # Integer columns
    clean_df['Serial'] = clean_df['Serial'].astype('int64')
    clean_df['Activity ID'] = pd.to_numeric(clean_df['Activity ID'], errors='coerce').fillna(0).astype('int64')
    
    # String/object columns
    clean_df['Athlete Name'] = clean_df['Athlete Name'].astype('object')
    clean_df['Type'] = clean_df['Type'].astype('object')
    clean_df['Start Date'] = clean_df['Start Date'].astype('object')
    
    # Float columns
    float_columns = ['Pace (min/mi)', 'Pace (min/km)', 'Time (min)', 
                    'Distance (km)', 'Activity Time (s)']
    for col in float_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0).astype('float64')
    
    # Reorder columns to match existing database
    required_columns = [
        'Serial',              # Integer (was 'Unnamed: 0')
        'Athlete ID',          # Integer (new addition)
        'Athlete Name',        # String/object
        'Activity ID',         # Integer
        'Activity Name',       # String/object (new addition)
        'Description',         # String/object (new addition)
        'Start Date',          # String/object
        'Elapsed Time',        # Integer (new addition)
        'Type',               # String/object
        'Location',           # String/object (new addition)
        'Pace (min/mi)',      # Float
        'Pace (min/km)',      # Float
        'Time (min)',         # Float
        'Distance (km)',      # Float
        'Activity Time (s)',  # Float
        'Time'                # String/object (new addition)
    ]
    
    for col in required_columns:
        if col not in clean_df.columns:
            if col in ['Athlete ID', 'Activity ID', 'Elapsed Time']:
                clean_df[col] = 0  # Integer default
            elif col in ['Pace (min/mi)', 'Pace (min/km)', 'Time (min)', 'Distance (km)', 'Activity Time (s)']:
                clean_df[col] = 0.0  # Float default
            else:
                clean_df[col] = ''  # String default for text columns
    
    # Ensure correct data types
    # Integer columns
    int_columns = ['Serial', 'Athlete ID', 'Activity ID', 'Elapsed Time']
    for col in int_columns:
        clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0).astype('int64')
    
    # String/object columns
    str_columns = ['Athlete Name', 'Activity Name', 'Description', 'Type', 'Location', 'Start Date', 'Time']
    for col in str_columns:
        clean_df[col] = clean_df[col].astype('object')
    
    # Float columns
    float_columns = ['Pace (min/mi)', 'Pace (min/km)', 'Time (min)', 'Distance (km)', 'Activity Time (s)']
    for col in float_columns:
        clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0).astype('float64')
    
    # Reorder columns to match required structure
    clean_df = clean_df[required_columns]
    
    # Validate final structure matches existing database
    if not all(clean_df.columns == expected_columns):
        logger.error("Column mismatch with existing database")
        logger.error(f"Expected: {expected_columns.tolist()}")
        logger.error(f"Got: {clean_df.columns.tolist()}")
        return pd.DataFrame()
    
    logger.info("Data cleaning completed successfully")
    logger.info(f"Processed {len(clean_df)} activities")
    return clean_df

def merge_new_activities(existing_activities_df: pd.DataFrame, new_activities_df: pd.DataFrame, target_athletes: list) -> pd.DataFrame:
    """
    Merge new activities with existing ones, handling time conversion properly
    """
    print("Merging activities...")
    
    # Clean new activities (includes time conversion)
    new_activities_clean = clean_activities_data(new_activities_df, target_athletes)
    
    # Clean existing activities (preserves existing Time (min))
    existing_activities_clean = clean_activities_data(existing_activities_df, target_athletes)
    
    # Combine dataframes
    combined_df = pd.concat([existing_activities_clean, new_activities_clean], ignore_index=True)
    print(f"Combined activities: {len(combined_df)} rows")
    
    return combined_df

def process_data(new_activities_df: pd.DataFrame, existing_activities_df: pd.DataFrame) -> pd.DataFrame:
    """Process and combine activity data at DataFrame level"""
    print("Processing activity data...")
    
    # Clean both dataframes
    new_activities_clean = clean_activities_data(new_activities_df)
    existing_activities_clean = clean_activities_data(existing_activities_df)
    
    # Combine dataframes
    combined_df = pd.concat([existing_activities_clean, new_activities_clean], ignore_index=True)
    print(f"Combined activities: {len(combined_df)} rows")
    
    # Calculate metrics using cleaned time in minutes
    metrics = calculate_athlete_metrics(combined_df)
    
    return metrics

def calculate_athlete_metrics(activities_df: pd.DataFrame, target_athletes: list) -> pd.DataFrame:
    """Calculate metrics for each athlete from activities"""
    logger.info("Calculating athlete metrics...")
    
    # Debug logging for input data
    logger.info(f"\nInput DataFrame shape: {activities_df.shape}")
    
    # Load metadata to get Athlete IDs
    try:
        metadata_df = pd.read_csv("../cleaned_athlete_metadata.csv")
        # Remove duplicates from target athletes by Athlete ID
        target_ids = metadata_df[metadata_df['Athlete Name'].isin(target_athletes)]['Athlete ID'].unique()
        logger.info(f"Target Athlete IDs: {target_ids}")
    except Exception as e:
        logger.error(f"Error loading metadata: {str(e)}")
        return pd.DataFrame()
    
    # Validate input data
    if activities_df.empty:
        logger.error("Empty activities DataFrame provided")
        return pd.DataFrame()
    
    if 'Athlete ID' not in activities_df.columns:
        logger.error("Athlete ID column missing from activities")
        return pd.DataFrame()
    
    # Filter activities for target athletes using IDs
    activities_df = activities_df[activities_df['Athlete ID'].isin(target_ids)]
    
    # Validate filtered data
    if activities_df.empty:
        logger.error("No activities found for target athletes")
        return pd.DataFrame()
    
    logger.info(f"\nFiltered DataFrame shape: {activities_df.shape}")
    logger.info(f"Activities found for athletes: {activities_df['Athlete Name'].unique()}")
    
    # Convert Time to Time (min) if it doesn't exist
    if 'Time (min)' not in activities_df.columns and 'Time' in activities_df.columns:
        logger.info("Converting 'Time' to minutes...")
        activities_df['Time (min)'] = activities_df['Time'].apply(time_to_minutes)
    
    # Group by athlete and activity type
    grouped = activities_df.groupby(['Athlete ID', 'Type']).agg({
        'Distance (km)': 'sum',
        'Time (min)': 'sum'
    }).reset_index()
    
    logger.info("\nGrouped data for target athletes:")
    for athlete_id in target_ids:
        athlete_data = grouped[grouped['Athlete ID'] == athlete_id]
        athlete_name = metadata_df[metadata_df['Athlete ID'] == athlete_id]['Athlete Name'].iloc[0]
        logger.info(f"\n{athlete_name} (ID: {athlete_id}):")
        logger.info(athlete_data)
    
    # Pivot for calculations
    pivot_dist = grouped.pivot(index='Athlete ID', 
                             columns='Type', 
                             values='Distance (km)').fillna(0).reset_index()
    pivot_time = grouped.pivot(index='Athlete ID', 
                             columns='Type', 
                             values='Time (min)').fillna(0).reset_index()
    
    # Calculate metrics - now using column access instead of index
    metrics = pd.DataFrame({
        'Athlete ID': pivot_dist['Athlete ID'],  # Use column instead of index
        'Total_Run_Distance_km': pivot_dist.get('Run', 0),
        'Avg_Weekly_Run_Mileage_km': pivot_dist.get('Run', 0) / 45,
        'Total_Run_Hours': pivot_time.get('Run', 0) / 60,
        'Avg_Weekly_Run_Hours': (pivot_time.get('Run', 0) / 60) / 45,
        'Avg_Run_Pace_min_per_km': pivot_time.get('Run', 0) / pivot_dist.get('Run', 0),
        'Total_Ride_Hours': pivot_time.get('Ride', 0) / 60,
        'Total_Swim_Hours': pivot_time.get('Swim', 0) / 60,
        'Total_Other_Hours': pivot_time.get('Other', 0) / 60
    }).round(2)
    
    # Add athlete names back using regular merge
    metrics = metrics.merge(
        metadata_df[['Athlete ID', 'Athlete Name']], 
        on='Athlete ID', 
        how='left'
    )
    
    return metrics

def update_athlete_metadata(metadata_df: pd.DataFrame, new_metrics_df: pd.DataFrame, start_week: int, end_week: int) -> pd.DataFrame:
    """Update athlete metadata with new metrics"""
    logger.info("Updating athlete metadata...")
    
    weeks_to_add = end_week - start_week
    logger.info(f"\nAdding {weeks_to_add} weeks (from week {start_week} to {end_week})")
    
    # For each athlete, update their metrics using Athlete ID
    for idx, new_metrics in new_metrics_df.iterrows():
        athlete_id = new_metrics['Athlete ID']
        athlete = new_metrics['Athlete Name']
        logger.info(f"\nUpdating {athlete} (ID: {athlete_id}):")
        
        # Update numeric columns
        numeric_columns = [
            'Total_Run_Distance_km',
            'Avg_Weekly_Run_Mileage_km',
            'Total_Run_Hours',
            'Avg_Weekly_Run_Hours',
            'Total_Ride_Hours',
            'Total_Swim_Hours',
            'Total_Other_Hours',
            'Avg_Run_Pace_min_per_km'
        ]
        
        # Update the values in metadata_df using Athlete ID
        for col in numeric_columns:
            metadata_df.loc[metadata_df['Athlete ID'] == athlete_id, col] = new_metrics[col]
            
        # Update weeks scraped
        current_weeks = metadata_df.loc[metadata_df['Athlete ID'] == athlete_id, '2024 Weeks Scraped'].values[0]
        metadata_df.loc[metadata_df['Athlete ID'] == athlete_id, '2024 Weeks Scraped'] = current_weeks + weeks_to_add
    
    return metadata_df

def print_specific_athletes(metrics_df: pd.DataFrame, athlete_names: list):
    """Print metrics for specific athlete names"""
    print("\nMetrics for specified athletes:")
    print("-" * 80)
    
    for athlete_name in athlete_names:
        athlete_data = metrics_df[metrics_df['Athlete Name'] == athlete_name]
        if not athlete_data.empty:
            print(f"\nAthlete: {athlete_name}")
            print(athlete_data.to_string(index=False))
        else:
            print(f"\nNo data found for athlete: {athlete_name}")



def update_databases(new_activities: pd.DataFrame, updated_master: pd.DataFrame, start_week: int, end_week: int) -> bool:
    """Update all three databases with new data"""
    try:
        # 1. Update indiv_activities_full.csv with validation
        existing_activities = pd.read_csv("../indiv_activities_full.csv")
        
        # Clean new activities first
        clean_new_activities = clean_activities_data(new_activities, updated_master['Competitor'].tolist())
        if clean_new_activities.empty:
            logger.error("Failed to clean new activities data")
            return False
            
        # Filter out activities that already exist
        new_unique_activities = clean_new_activities[
            ~clean_new_activities['Activity ID'].isin(existing_activities['Activity ID'])
        ]
        
        if not new_unique_activities.empty:
            # Log new activities being added
            logger.info("\nNew activities to be appended:")
            for athlete in new_unique_activities['Athlete Name'].unique():
                athlete_activities = new_unique_activities[new_unique_activities['Athlete Name'] == athlete]
                logger.info(f"\n{athlete}:")
                logger.info(f"Number of activities: {len(athlete_activities)}")
                logger.info("\nActivity details:")
                for _, activity in athlete_activities.iterrows():
                    logger.info(
                        f"- {activity['Start Date']} | "
                        f"{activity['Type']} | "
                        f"Distance: {activity['Distance (km)']}km | "
                        f"Time: {activity['Time (min)']}min | "
                        f"Activity ID: {activity['Activity ID']}"
                    )
            
            # Append to indiv_activities_full.csv
            with open("../indiv_activities_full.csv", 'a') as f:
                new_unique_activities.to_csv(f, header=False, index=False)
            logger.info(f"\nSuccessfully added {len(new_unique_activities)} new activities")

            # 2. Calculate metrics using all activities
            all_activities = pd.concat([existing_activities, new_unique_activities], ignore_index=True)
            athlete_metrics = calculate_athlete_metrics(all_activities, updated_master['Competitor'].tolist())
            
            # 3. Update metadata with new metrics
            if not athlete_metrics.empty:
                metadata_df = pd.read_csv("../cleaned_athlete_metadata.csv")
                metadata_df = update_athlete_metadata(metadata_df, athlete_metrics, start_week, end_week)
                metadata_df.to_csv("../cleaned_athlete_metadata.csv", index=False)
                logger.info("Updated athlete metrics in metadata")
            
            # 4. Update master database weeks scraped
            master_df = pd.read_csv("../data/metadata/master_iaaf_database_with_strava.csv")
            updates_made = False
            for idx, row in updated_master.iterrows():
                mask = master_df['Athlete ID'] == row['Athlete ID']
                if any(mask):
                    master_df.loc[mask, '2024 Weeks Scraped'] = row['2024 Weeks Scraped']
                    updates_made = True
            
            if updates_made:
                master_df.to_csv("../data/metadata/master_iaaf_database_with_strava.csv", index=False)
                logger.info("Updated weeks scraped in master database")

        else:
            logger.info("No new unique activities to add")

        return True

    except Exception as e:
        logger.error(f"Error updating databases: {str(e)}")
        return False

def process_data(start_week: int = 45, end_week: int = 47):
    """Main function to process data and update databases"""
    logger.info(f"Starting data processing for weeks {start_week}-{end_week}")
    
    # Get new data from Strava
    new_activities, updated_master = check_data_updates(start_week, end_week)
    
    if new_activities is not None and not new_activities.empty:
        # Update all databases
        success = update_databases(new_activities, updated_master, start_week, end_week)
        if success:
            logger.info("Successfully updated all databases")
            return True
        else:
            logger.error("Failed to update databases")
            return False
    else:
        logger.warning("No new activities to process")
        return False

if __name__ == "__main__":
    process_data()

