import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime
import sys
import traceback

def setup_logging():
    """Setup logging to both file and console"""
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Setup file handler
    file_handler = logging.FileHandler('../logs/iaaf_update.log')
    file_handler.setFormatter(formatter)
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def get_full_event_results(base_url, start_page, discipline, cutoff_score=1100):
    page = start_page
    results = []
    stop_collecting = False

    while True:
        url = base_url.replace("&page=1", f"&page={page}")
        response = requests.get(url)
        data = response.text
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table')

        if not table:
            break

        rows = table.find_all('tr')[1:]

        if not rows:
            break

        for row in rows:
            cols = row.find_all('td')
            if len(cols) > 1:
                mark = cols[1].text.strip()
                competitor = cols[3].text.strip()
                nationality = cols[5].text.strip()
                location = cols[-3].text.strip()
                date = cols[-2].text.strip()
                result_score = cols[-1].text.strip()

                # Convert result_score to int, skip if not possible
                try:
                    result_score = int(result_score)
                except ValueError:
                    continue

                if result_score < cutoff_score:
                    stop_collecting = True
                    break

                results.append({
                    'Mark': mark,
                    'Competitor': competitor,
                    'Nat': nationality,
                    'Location': location,
                    'Date': date,
                    'Results Score': result_score,  # Now storing as int
                    'Discipline': discipline
                })

        if stop_collecting:
            break

        page += 1

    df = pd.DataFrame(results)
    return df

def collect_multiple_events(event_list):
    """
    Collect results from multiple events with progress tracking
    """
    dfs = []
    total_events = len(event_list)
    
    for idx, event in enumerate(event_list, 1):
        base_url = event['base_url']
        discipline = event['discipline']
        
        # Log progress
        logging.info(f"Processing {discipline} ({idx}/{total_events})")
        
        try:
            df = get_full_event_results(base_url, start_page=1, discipline=discipline)
            dfs.append(df)
            logging.info(f"Successfully collected {len(df)} results for {discipline}")
        except Exception as e:
            logging.error(f"Error collecting {discipline}: {str(e)}")
            continue  # Continue with next event even if one fails
    
    if not dfs:  # If no data was collected
        raise Exception("No data collected from any events")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

#basically compare updated list with existing db, will include road races as well (closer to EOY)
event_list_male = [
    {
        'discipline': '800m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/800-metres/all/men/senior/2024?regionType=world&timing=electronic&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229501&ageCategory=senior'
    },
    {
        'discipline': '1500m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/1500-metres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229502&ageCategory=senior'
    },
    {
        'discipline': '3000m Steeple',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/3000-metres-steeplechase/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229614&ageCategory=senior'
    },
    {
        'discipline': '5000m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/5000-metres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229609&ageCategory=senior'
    },
    {
        'discipline': '10000m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/10000-metres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229610&ageCategory=senior'
    }, 
    {
        'discipline': 'Half',
        'base_url': "https://worldathletics.org/records/toplists/road-running/half-marathon/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229633&ageCategory=senior"
    },
    {
        'discipline': 'Full',
        'base_url': "https://worldathletics.org/records/toplists/road-running/marathon/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229634&ageCategory=senior"
    }, 
    {
        'discipline': '5k Road',
        'base_url': 'https://worldathletics.org/records/toplists/road-running/5-kilometres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=204597&ageCategory=senior'
    },
    {
        'discipline': '10k Road',
        'base_url': 'https://worldathletics.org/records/toplists/road-running/10-kilometres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229507&ageCategory=senior'
    },
    {
        'discipline': '15k Road',
        'base_url': 'https://worldathletics.org/records/toplists/road-running/15-kilometres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229504&ageCategory=senior'
    },
    {
        'discipline': '10M Road',
        'base_url': 'https://worldathletics.org/records/toplists/road-running/10-miles-road/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229505&ageCategory=senior'
    },
    {
        'discipline': '20k Road',
        'base_url': 'https://worldathletics.org/records/toplists/road-running/20-kilometres/all/men/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229506&ageCategory=senior'
    }
]

event_list_female = [
   {
        'discipline': '800m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/800-metres/all/women/senior/2024?regionType=world&timing=electronic&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229512&ageCategory=senior'
    },
    {
        'discipline': '1500m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/1500-metres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229513&ageCategory=senior'
    },
    {
        'discipline': '3000m Steeple',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/3000-metres-steeplechase/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229524&ageCategory=senior'
    },
    {
        'discipline': '5000m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/5000-metres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229514&ageCategory=senior'
    },
    {
        'discipline': '10000m',
        'base_url': 'https://worldathletics.org/records/toplists/middlelong/10000-metres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229521&ageCategory=senior'
    },
    {
        'discipline': 'Half',
        'base_url': "https://worldathletics.org/records/toplists/road-running/half-marathon/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229541&ageCategory=senior"
    },
    {
        'discipline': 'Full',
        'base_url': "https://worldathletics.org/records/toplists/road-running/marathon/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229534&ageCategory=senior"
    },
    {
        'discipline': '5k Road',
        'base_url': "https://worldathletics.org/records/toplists/road-running/5-kilometres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=204598&ageCategory=senior"
    },
    {
        'discipline': '10k Road',
        'base_url': "https://worldathletics.org/records/toplists/road-running/10-kilometres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229537&ageCategory=senior"
    },
    {
        'discipline': '15k Road',
        'base_url': "https://worldathletics.org/records/toplists/road-running/15-kilometres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229538&ageCategory=senior"
    },
    {
        'discipline': '10M Road',
        'base_url': "https://worldathletics.org/records/toplists/road-running/10-miles-road/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229539&ageCategory=senior"
    },
    {
        'discipline': '20k Road',
        'base_url': "https://worldathletics.org/records/toplists/road-running/20-kilometres/all/women/senior/2024?regionType=world&page=1&bestResultsOnly=true&maxResultsByCountry=all&eventId=10229540&ageCategory=senior"
    },
]

def update_iaaf_database():
    """
    Main function to update the IAAF database with new performances.
    Returns True if successful, False if there was an error.
    """
    # Set up logging
    logging.basicConfig(
        filename='../logs/iaaf_update.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        logging.info("Starting IAAF database update")
        
        # Read the master database
        master_df = pd.read_csv('../data/metadata/master_iaaf_database.csv')
        # Convert Results Score to int in master_df
        master_df['Results Score'] = pd.to_numeric(master_df['Results Score'], errors='coerce')
        master_df = master_df.dropna(subset=['Results Score'])
        master_df['Results Score'] = master_df['Results Score'].astype(int)
        
        logging.info(f"Loaded master database with {len(master_df)} entries")

        # Create Performance_ID
        master_df['Performance_ID'] = master_df.apply(
            lambda x: f"{x['Competitor']}_{x['Discipline']}_{x['Mark']}_{x['Date']}", 
            axis=1
        )

        # Add Date_Created if not present
        if 'Date_Created' not in master_df.columns:
            master_df['Date_Created'] = "30 Oct 24"

        # Get new results with progress tracking
        logging.info("Starting collection of male results...")
        male_results = collect_multiple_events(event_list_male)
        
        logging.info("Starting collection of female results...")
        female_results = collect_multiple_events(event_list_female)

        # Get new results
        try:
            male_results = collect_multiple_events(event_list_male)
            female_results = collect_multiple_events(event_list_female)
            logging.info(f"Successfully collected new results - Male: {len(male_results)}, Female: {len(female_results)}")
        except Exception as e:
            logging.error(f"Error collecting results: {str(e)}")
            raise

        # Add Gender and combine results
        male_results['Gender'] = 'M'
        female_results['Gender'] = 'F'
        new_results = pd.concat([male_results, female_results], ignore_index=True)

        # Filter for scores >= 1100
        new_results = new_results[new_results['Results Score'] >= 1100]

        # Create Performance_ID for new results
        new_results['Performance_ID'] = new_results.apply(
            lambda x: f"{x['Competitor']}_{x['Discipline']}_{x['Mark']}_{x['Date']}", 
            axis=1
        )

        # Add current date as Date_Created
        current_date = datetime.now().strftime("%d %b %y")
        new_results['Date_Created'] = current_date

        # Find new performances
        new_performances = new_results[~new_results['Performance_ID'].isin(master_df['Performance_ID'])]

        # Combine and sort
        updated_df = pd.concat([master_df, new_performances], ignore_index=True)
        updated_df = updated_df.sort_values('Results Score', ascending=False)
        updated_df = updated_df.drop('Performance_ID', axis=1)

        # Backup existing database
        master_df.to_csv(f'../data/metadata/backup/master_iaaf_database_{datetime.now().strftime("%Y%m%d")}.csv', index=False)
        
        # Save updated database
        updated_df.to_csv('../data/metadata/master_iaaf_database.csv', index=False)

        # Log statistics
        logging.info(f"""
        Update Summary:
        Original entries: {len(master_df)}
        New entries added: {len(new_performances)}
        Total entries: {len(updated_df)}
        New entries by discipline:\n{new_performances['Discipline'].value_counts().to_string()}
        New entries by gender:\n{new_performances['Gender'].value_counts().to_string()}
        """)

        return True

    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = update_iaaf_database()
    sys.exit(0 if success else 1)

#script for creating IAAF db 
"""

# Read both CSV files
female_df = pd.read_csv('../data/metadata/combined_df_female_distanceCAA30Oct.csv')
male_df = pd.read_csv('../data/metadata/combined_df_male_distanceCAA30Oct (1).csv')

# Add a 'Gender' column to each DataFrame
female_df['Gender'] = 'F'
male_df['Gender'] = 'M'

# Concatenate the DataFrames
merged_df = pd.concat([female_df, male_df], ignore_index=True)

# Filter for scores >= 1100
master_df = merged_df[merged_df['Results Score'] >= 1100]

# Sort by Results Score in descending order
master_df = master_df.sort_values('Results Score', ascending=False)

# Save to new CSV file
master_df.to_csv('../data/metadata/master_iaaf_database.csv', index=False)

# Print some statistics
print(f"Total number of performances ≥ 1100 points: {len(master_df)}")
print(f"Number of female performances: {len(master_df[master_df['Gender'] == 'F'])}")
print(f"Number of male performances: {len(master_df[master_df['Gender'] == 'M'])}")
print(f"\nEvents covered: {master_df['Discipline'].unique()}")
"""

# doing the following might result in recording some additional athletes being duplicated
