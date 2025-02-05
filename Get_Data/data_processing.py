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


def clean_activities_data(raw_activities: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert activity data types"""
    
    target_ids = raw_activities['Athlete ID'].unique()
    logger.info(f"Processing activities for {len(target_ids)} athletes")
    logger.info(f"Athlete IDs being processed: {sorted(target_ids)}")
    # Read existing database to get last serial number and validate structure
    try:
        existing_df = pd.read_csv("../indiv_activities_full.csv")
        last_serial = existing_df.iloc[-1, 0] if not existing_df.empty else 0
        expected_columns = existing_df.columns
        existing_ids = existing_df['Athlete ID'].unique()
        new_ids = set(target_ids) - set(existing_ids)
        if new_ids:
            logger.warning(f"New athlete IDs not in existing database: {sorted(new_ids)}")
            
    except Exception as e:
        logger.error(f"Error reading existing database: {str(e)}")
        return pd.DataFrame()

    # Make a copy to avoid modifying original
    clean_df = raw_activities.copy()
    
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


def update_activities_database(new_activities: pd.DataFrame) -> bool:
    """Update indiv_activities_full.csv with new cleaned activities"""
    try:
        # Read existing database
        existing_activities = pd.read_csv("../indiv_activities_full.csv")
        logger.info(f"Existing activities: {len(existing_activities)}")
        
        # Filter out activities that already exist
        new_unique_activities = new_activities[
            ~new_activities['Activity ID'].isin(existing_activities['Activity ID'])
        ]
        
        if new_unique_activities.empty:
            logger.info("No new unique activities to add")
            return True
            
        # Log new activities being added
        logger.info("\nNew activities to be appended:")
        for athlete in new_unique_activities['Athlete Name'].unique():
            athlete_activities = new_unique_activities[new_unique_activities['Athlete Name'] == athlete]
            logger.info(f"{athlete}: {len(athlete_activities)} activities")
        
        # Append to database
        with open("../indiv_activities_full.csv", 'a') as f:
            new_unique_activities.to_csv(f, header=False, index=False)
        
        logger.info(f"Successfully added {len(new_unique_activities)} new activities")
        return True

    except Exception as e:
        logger.error(f"Error updating activities database: {str(e)}")
        return False

def calculate_athlete_metrics(target_ids: list, end_week: int) -> pd.DataFrame:
    """Calculate metrics from indiv_activities_full for target athletes"""
    try:
        # Read complete activities database
        activities_df = pd.read_csv("../indiv_activities_full.csv")
        
        logger.info(f"Calculating metrics for athlete IDs: {sorted(target_ids)}")
        
        # Filter activities for target athletes
        activities_df = activities_df[activities_df['Athlete ID'].isin(target_ids)]
        
        if activities_df.empty:
            logger.error("No activities found for target IDs")
            return pd.DataFrame()
        
        # Group by athlete and activity type
        grouped = activities_df.groupby(['Athlete ID', 'Type']).agg({
            'Distance (km)': 'sum',
            'Time (min)': 'sum'
        }).reset_index()
        
        # Pivot for calculations
        pivot_dist = grouped.pivot(index='Athlete ID', 
                                 columns='Type', 
                                 values='Distance (km)').fillna(0).reset_index()
        pivot_time = grouped.pivot(index='Athlete ID', 
                                 columns='Type', 
                                 values='Time (min)').fillna(0).reset_index()
        
        # Calculate metrics
        metrics = pd.DataFrame({
            'Athlete ID': pivot_dist['Athlete ID'],
            'Total_Run_Distance_km': pivot_dist.get('Run', 0),
            'Avg_Weekly_Run_Mileage_km': pivot_dist.get('Run', 0) / end_week, #this needs to be dynamically handled
            'Total_Run_Hours': pivot_time.get('Run', 0) / 60,
            'Avg_Weekly_Run_Hours': (pivot_time.get('Run', 0) / 60) / end_week, #this needs to be dynamically handled 
            'Avg_Run_Pace_min_per_km': pivot_time.get('Run', 0) / pivot_dist.get('Run', 0),
            'Total_Ride_Hours': pivot_time.get('Ride', 0) / 60,
            'Total_Swim_Hours': pivot_time.get('Swim', 0) / 60,
            'Total_Other_Hours': pivot_time.get('Other', 0) / 60
        }).round(2)
        
        return metrics

    except Exception as e:
        logger.error(f"Error calculating metrics: {str(e)}")
        return pd.DataFrame()

def update_metadata_databases(new_metrics: pd.DataFrame, start_week: int, end_week: int) -> bool:
    """Update both metadata files with new information"""
    try:
        # 1. Update cleaned_athlete_metadata
        metadata_df = pd.read_csv("../cleaned_athlete_metadata.csv")
        
        # Update metrics for each athlete
        for _, new_row in new_metrics.iterrows():
            athlete_id = new_row['Athlete ID']
            mask = metadata_df['Athlete ID'] == athlete_id
            
            # Update numeric columns
            numeric_columns = [col for col in new_metrics.columns 
                             if col not in ['Athlete ID', 'Athlete Name']]
            for col in numeric_columns:
                metadata_df.loc[mask, col] = new_row[col]
            
            weeks_to_add = end_week - start_week
            current_weeks = metadata_df.loc[mask, '2024 Weeks Scraped'].iloc[0]
            metadata_df.loc[mask, '2024 Weeks Scraped'] = current_weeks + weeks_to_add
        
        # Save updated metadata
        metadata_df.to_csv("../cleaned_athlete_metadata.csv", index=False)
        
        # 2. Update master database weeks scraped
        master_df = pd.read_csv("../data/metadata/master_iaaf_database_with_strava.csv")
        weeks_to_add = end_week - start_week
        
        for athlete_id in new_metrics['Athlete ID']:
            mask = master_df['Athlete ID'] == athlete_id
            current_weeks = master_df.loc[mask, '2024 Weeks Scraped'].iloc[0]
            master_df.loc[mask, '2024 Weeks Scraped'] = current_weeks + weeks_to_add
        
        master_df.to_csv("../data/metadata/master_iaaf_database_with_strava.csv", index=False)
        return True

    except Exception as e:
        logger.error(f"Error updating metadata databases: {str(e)}")
        return False

def process_data(start_week: int, end_week: int, raw_activities_path: str, target_ids: list) -> bool:
    """Main processing function
    
    Args:
        start_week: Starting week number to process
        end_week: Ending week number to process
        raw_activities_path: Path to CSV containing raw Strava activities
        target_ids: List of athlete IDs to process
    """
    try:
        # 1. Get and clean new activities
        raw_activities = pd.read_csv(raw_activities_path)
        new_activities = clean_activities_data(raw_activities)
        
        # 2. Update activities database
        if not update_activities_database(new_activities):
            logger.error("Failed to update activities database")
            return False
            
        # 3. Calculate new metrics using only target_ids
        new_metrics = calculate_athlete_metrics(target_ids, end_week)
        if new_metrics.empty:
            logger.error("Failed to calculate new metrics")
            return False
        
        # 4. Update metadata databases
        return update_metadata_databases(new_metrics, start_week, end_week)
        
    except Exception as e:
        logger.error(f"Error in process_data: {str(e)}")
        return False


