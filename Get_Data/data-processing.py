import pandas as pd
from datetime import datetime

"""
# Read the existing CSV
df = pd.read_csv('../cleaned_athlete_metadata.csv')

# Add new column with value 45 for all athletes
df['2024 Weeks Scraped'] = 45

# Save back to CSV, preserving existing format
df.to_csv('../cleaned_athlete_metadata.csv', index=False)

"""
import pandas as pd
from datetime import datetime

# Read master IAAF database with Strava, preserving NA values
#master_df = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv', keep_default_na=False)



# Set weeks scraped based on conditions
def determine_weeks_scraped(row):
    if row['Profile Visibility'] == 'Public':
        if row['Date_Created'] == "15 Jan 25":
            return 0
        elif row['Date_Created'] == "30 Oct 24":
            return 45
    return 'NA'

# Add new column without modifying existing ones
#master_df['2024 Weeks Scraped'] = master_df.apply(determine_weeks_scraped, axis=1)


# Save back to CSV, preserving existing format
#master_df.to_csv('../data/metadata/master_iaaf_database_with_strava.csv', index=False)

import pandas as pd
import os

def merge_new_activities(existing_activities_path: str, new_activities_path: str) -> pd.DataFrame:
    """
    Merge new activities with existing activities at DataFrame level
    """
    print("Loading activity data...")
    
    # Read existing and new activities
    existing_df = pd.read_csv(existing_activities_path)
    new_df = pd.read_csv(new_activities_path)
    
    # Combine dataframes
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    print(f"Combined activities: {len(combined_df)} rows")
    
    return combined_df

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

def clean_activities_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert activity data types"""
    print("Cleaning activity data...")
    
    # Make a copy to avoid modifying original
    clean_df = df.copy()
    
    # Convert Distance to float
    clean_df['Distance (km)'] = pd.to_numeric(clean_df['Distance (km)'], errors='coerce').fillna(0).astype(float)
    
    # Convert Time to minutes
    clean_df['Time (min)'] = clean_df['Time'].apply(time_to_minutes)
    
    print("Data cleaning completed")
    return clean_df

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

def calculate_athlete_metrics(activities_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate metrics for each athlete from activities"""
    print("Calculating athlete metrics...")
    
    # Group by athlete and activity type
    grouped = activities_df.groupby(['Athlete Name', 'Type']).agg({
        'Distance (km)': 'sum',
        'Time (min)': 'sum'  # Now using minutes
    }).reset_index()
    
    # Pivot for calculations
    pivot_dist = grouped.pivot(index='Athlete Name', 
                             columns='Type', 
                             values='Distance (km)').fillna(0)
    pivot_time = grouped.pivot(index='Athlete Name', 
                             columns='Type', 
                             values='Time (min)').fillna(0)
    
    # Calculate metrics (converting minutes to hours where needed)
    metrics = pd.DataFrame({
        'Athlete Name': pivot_dist.index,
        'Total_Run_Distance_km': pivot_dist.get('Run', 0),
        'Avg_Weekly_Run_Mileage_km': pivot_dist.get('Run', 0) / 45,
        'Total_Run_Hours': pivot_time.get('Run', 0) / 60,  # Convert to hours
        'Avg_Weekly_Run_Hours': (pivot_time.get('Run', 0) / 60) / 45,
        'Total_Ride_Hours': pivot_time.get('Ride', 0) / 60,
        'Total_Swim_Hours': pivot_time.get('Swim', 0) / 60,
        'Total_Other_Hours': pivot_time.get('Other', 0) / 60
    }).round(2)
    
    print(f"Processed metrics for {len(metrics)} athletes")
    return metrics

def print_specific_athletes(metrics_df: pd.DataFrame, athlete_names: list):
    """
    Print metrics for specific athlete names
    """
    print("\nMetrics for specified athletes:")
    print("-" * 80)
    
    for athlete_name in athlete_names:
        athlete_data = metrics_df[metrics_df['Athlete Name'] == athlete_name]
        if not athlete_data.empty:
            print(f"\nAthlete: {athlete_name}")
            print(athlete_data.to_string(index=False))
        else:
            print(f"\nNo data found for athlete: {athlete_name}")

def process_data():
    """
    Main function to process data
    """
    # File paths
    new_activities_path = "../data/tempdata/processed_AdamFOGG_to_ThomasBRIDGER_w50-52_20250125_160050.csv"
    existing_activities_path = "../indiv_activities_full.csv"
    
    # 1. Merge new activities with existing ones
    combined_activities = merge_new_activities(existing_activities_path, new_activities_path)
    
    # 2. Calculate updated metrics
    updated_metrics = calculate_athlete_metrics(combined_activities)
    
    # Print metrics for specific athletes
    target_athletes = ['Adam Fogg', 'JP Flavin', 'Thomas Bridger']
    print_specific_athletes(updated_metrics, target_athletes)
    
    return updated_metrics

if __name__ == "__main__":
    updated_df = process_data()

if __name__ == "__main__":
    updated_df = process_data()
