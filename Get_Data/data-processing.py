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
master_df = pd.read_csv('../data/metadata/master_iaaf_database_with_strava.csv', keep_default_na=False)



# Set weeks scraped based on conditions
def determine_weeks_scraped(row):
    if row['Profile Visibility'] == 'Public':
        if row['Date_Created'] == "15 Jan 25":
            return 0
        elif row['Date_Created'] == "30 Oct 24":
            return 45
    return 'NA'

# Add new column without modifying existing ones
master_df['2024 Weeks Scraped'] = master_df.apply(determine_weeks_scraped, axis=1)


# Save back to CSV, preserving existing format
master_df.to_csv('../data/metadata/master_iaaf_database_with_strava.csv', index=False)