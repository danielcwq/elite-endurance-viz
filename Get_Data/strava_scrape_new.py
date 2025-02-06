#better way instead of git ignoring is really to us os variables and then work up lol 
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
import time
import logging
import os
import json 
import re 
import threading
from queue import Queue
import html
from datetime import datetime
from selenium import webdriver
import pandas as pd
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

load_dotenv()
TEMP_DATA_DIR = '../data/tempdata'

def setup_logging():
    """Setup logging to both file and console"""
    # Create logs directory if it doesn't exist
    os.makedirs('../logs', exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Setup file handler
    file_handler = logging.FileHandler('../logs/strava_login.log')
    file_handler.setFormatter(formatter)
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def web_driver():
    """Initialize and return a Chrome WebDriver"""
    logging.info("Initializing Chrome WebDriver")
    try:
        options = Options()
        options.add_argument("--verbose")
        options.add_argument('--disable-gpu')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--kiosk")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        logging.info("WebDriver initialized successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

def login_strava(driver, username=None, password=None):
    """
    Attempt to log in to Strava with validation checks and wait times for human intervention
    Returns True if successful, False otherwise
    """
    try:
        logging.info("Attempting to login to Strava")
        driver.get("https://www.strava.com/")
        
        # Find and click login button
        login = driver.find_element(By.LINK_TEXT, "Log In")
        login.click()
        logging.info("Clicked login button")

        # Setup wait
        wait = WebDriverWait(driver, 10)
        
        # Use provided credentials or fall back to defaults
        username = username or os.getenv('STRAVA_EMAIL_1')
        password = password or os.getenv('STRAVA_PASSWORD')

        # Step 1: Enter Email
        try:
            name_box = wait.until(EC.element_to_be_clickable((By.ID, "desktop-email")))
            name_box.clear()
            name_box.send_keys(username)
            logging.info("Entered username")
        except Exception as e:
            logging.error(f"Error entering email: {str(e)}")
            logging.info("Waiting 20 seconds for human intervention...")
            time.sleep(20)
            # Try one more time
            name_box = wait.until(EC.element_to_be_clickable((By.ID, "desktop-email")))
            name_box.clear()
            name_box.send_keys(username)

        # Click first login button
        try:
            buttons = driver.find_elements(
                By.CSS_SELECTOR,
                'button[class*="EmailLoginForm_submitButton"]'
            )
            login_button = next(
                (button for button in buttons if button.text.strip() == "Log In"),
                None
            )
            if login_button:
                login_button.click()
            else:
                raise Exception("Login button with text 'Log In' not found")
        except Exception as e:
            logging.error(f"Error clicking first login button: {str(e)}")
            logging.info("Waiting 20 seconds for human intervention...")
            time.sleep(20)
            # Try one more time
            buttons = driver.find_elements(
                By.CSS_SELECTOR,
                'button[class*="EmailLoginForm_submitButton"]'
            )
            login_button = next(
                (button for button in buttons if button.text.strip() == "Log In"),
                None
            )
            if login_button:
                login_button.click()
            else:
                return False

        time.sleep(10)

        # Step 2: Enter Password
        try:
            visible_elements = driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]')
            password_entered = False
            for element in visible_elements:
                if element.is_displayed():
                    element.click()
                    element.clear()
                    element.send_keys(password)
                    password_entered = True
                    break
            if not password_entered:
                raise Exception("Password field not found")
            logging.info("Entered password")
        except Exception as e:
            logging.error(f"Error entering password: {str(e)}")
            logging.info("Waiting 20 seconds for human intervention...")
            time.sleep(20)
            # Try one more time
            visible_elements = driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]')
            for element in visible_elements:
                if element.is_displayed():
                    element.click()
                    element.clear()
                    element.send_keys(password)
                    break

        # Click final login button
        try:
            buttons = driver.find_elements(
                By.CSS_SELECTOR,
                'button[class*="Button_btn__GRPGo Button_primary__xpM4p OTPCTAButton_ctaButton__i3OwD"]'
            )
            login_button = next(
                (button for button in buttons if button.text.strip() == "Log in"),
                None
            )
            if login_button:
                login_button.click()
            else:
                raise Exception("Final login button not found")
        except Exception as e:
            logging.error(f"Error clicking final login button: {str(e)}")
            logging.info("Waiting 20 seconds for human intervention...")
            time.sleep(20)
            # Try one more time
            buttons = driver.find_elements(
                By.CSS_SELECTOR,
                'button[class*="Button_btn__GRPGo Button_primary__xpM4p OTPCTAButton_ctaButton__i3OwD"]'
            )
            login_button = next(
                (button for button in buttons if button.text.strip() == "Log in"),
                None
            )
            if login_button:
                login_button.click()
            else:
                return False

        time.sleep(10)
        
        # Final validation
        try:
            wait.until(lambda driver: 
                len(driver.find_elements(By.CLASS_NAME, "getting-started")) > 0 or 
                len(driver.find_elements(By.CLASS_NAME, "error")) > 0
            )
            
            if len(driver.find_elements(By.CLASS_NAME, "getting-started")) > 0:
                logging.info("Login successful - getting-started element found")
                return True
            else:
                logging.error("Login failed - error element found")
                logging.info("Waiting 20 seconds for final human intervention...")
                time.sleep(20)
                # Check one last time
                if len(driver.find_elements(By.CLASS_NAME, "getting-started")) > 0:
                    logging.info("Login successful after human intervention")
                    return True
                return False
                
        except Exception as e:
            logging.error("Login verification timed out")
            logging.info("Waiting 20 seconds for final human intervention...")
            time.sleep(20)
            # Final check
            if len(driver.find_elements(By.CLASS_NAME, "getting-started")) > 0:
                logging.info("Login successful after human intervention")
                return True
            return False

    except Exception as e:
        logging.error(f"Login failed: {str(e)}")
        return False
    

# Helper functions
def preprocess_json_string(json_str):
    """
    Preprocess the JSON string by unescaping HTML entities and removing control characters.
    """
    # Unescape HTML entities
    json_str = html.unescape(json_str)
    # Remove control characters outside of string values
    json_str = re.sub(r'[\n\r\t]', '', json_str)
    # Remove extra commas before closing braces/brackets
    json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
    return json_str.strip()
def consolidate_weekly_data(driver, df, start_week=1, end_week=40):
    """
    Consolidate weekly data and collect JSON data for individual activities.

    Parameters:
    - df (pd.DataFrame): DataFrame containing 'Athlete ID' and 'Competitor' columns.
    - start_week (int): Starting week number.
    - end_week (int): Ending week number.

    Returns:
    - df_weekly (pd.DataFrame): DataFrame containing weekly metadata.
    - df_json (pd.DataFrame): DataFrame containing JSON data for post-processing.
    """
    # Ensure the 'driver' variable is available

    # Lists to hold weekly data
    athlete_ids = []
    names = []
    week_numbers = []
    date_ranges = []
    distances = []
    times_list = []
    elevations = []

    # List to hold JSON data
    json_data_list = []

    year = 2024  # Adjust the year as needed

    # Loop over each athlete
    for idx, row in df.iterrows():
        athlete_id = str(row['Athlete ID'])
        name = row['Competitor']
        print(f"Starting processing for athlete {idx+1}/{len(df)}: {name} (ID: {athlete_id})")

        # Navigate to the athlete's main page
        athlete_url = f'https://www.strava.com/athletes/{athlete_id}'  

        for week_number in range(start_week, end_week + 1):
            week_number_str = f"{week_number:02d}"
            week_id = f"interval-{year}{week_number_str}"
            print(f"\nProcessing Week ID: {week_id}")

            # Navigate to the athlete's main page each time
            driver.get(athlete_url)
            time.sleep(1)

            try:
                # Find the week element
                week_element = driver.find_element(By.ID, week_id)

                # Click on the week link
                link_element = week_element.find_element(By.TAG_NAME, 'a')
                link_element.click()
                time.sleep(3)  # Wait for the page to load

                # Extract weekly metadata
                # Wait for date range element
                date_range_element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, "interval-value"))
                )
                date_range = date_range_element.text

                # Extract totals
                totals_section = driver.find_element(By.ID, "totals")
                total_values = totals_section.find_elements(By.TAG_NAME, "strong")

                # Extract distance
                distance_raw = total_values[0].text.strip()
                match = re.match(r'([\d,\.]+)\s*([a-zA-Z]+)', distance_raw)
                if match:
                    distance_value_str = match.group(1).replace(',', '')
                    unit = match.group(2)
                    distance_value = float(distance_value_str)
                else:
                    distance_value = 0.0
                    unit = None

                if unit == 'mi':
                    distance_km = distance_value * 1.60934
                elif unit == 'km':
                    distance_km = distance_value
                else:
                    distance_km = 0.0

                # Extract time
                time_value = total_values[1].text.strip()

                # Extract elevation
                elevation_raw = total_values[2].text.strip()
                match_elevation = re.match(r'([\d,\.]+)\s*([a-zA-Z]+)', elevation_raw)
                if match_elevation:
                    elevation_value_str = match_elevation.group(1).replace(',', '')
                    elevation_unit = match_elevation.group(2)
                    elevation_value = float(elevation_value_str)
                else:
                    elevation_value = 0.0
                    elevation_unit = None

                if elevation_unit == 'ft':
                    elevation_meters = elevation_value * 0.3048
                elif elevation_unit == 'm':
                    elevation_meters = elevation_value
                else:
                    elevation_meters = 0.0

                # Append data to lists
                athlete_ids.append(athlete_id)
                names.append(name)
                week_numbers.append(week_number_str)
                date_ranges.append(date_range)
                distances.append(distance_km)
                times_list.append(time_value)
                elevations.append(elevation_meters)

                print(f"Week {week_number_str}: Distance {distance_km} km, Time {time_value}, Elevation {elevation_meters} m")

                # Extract JSON data for individual activities
                try:
                    # Wait for the page to load fully
                    time.sleep(2)  # Adjust as needed

                    # Find all elements with data-react-props
                    elements = driver.find_elements(By.CSS_SELECTOR, '[data-react-props]')

                    data_react_props_found = False

                    # Loop through the elements to identify the right one
                    for element in elements:
                        # Retrieve the data-react-props attribute
                        data_react_props = element.get_attribute("data-react-props")

                        # Unescape HTML entities if necessary
                        data_react_props_unescaped = html.unescape(data_react_props)

                        # Parse it as JSON
                        try:
                            data = json.loads(data_react_props_unescaped)
                        except json.JSONDecodeError:
                            continue  # Skip this element if JSON decoding fails

                        # Check if 'appContext' contains 'athleteProfileId'
                        if "appContext" in data and "athleteProfileId" in data["appContext"]:
                            # Found the desired data
                            data_react_props_found = True
                            break  # Exit the loop

                    if not data_react_props_found:
                        print(f"Week {week_number_str}: Desired data not found in data-react-props.")
                        continue  # Proceed to the next week

                    # Store the JSON data for post-processing
                    json_data_list.append({
                        'Athlete ID': athlete_id,
                        'Name': name,
                        'Week Number': week_number_str,
                        'Date Range': date_range,
                        'JSON Data': data_react_props_unescaped
                    })

                except Exception as e:
                    print(f"Week {week_number_str}: Error collecting JSON data: {e}")
                    # Continue to next week

                # Wait before the next iteration
                time.sleep(3)

            except Exception as e:
                # Handle cases where there are no workouts or elements are not found
                print(f"Week {week_number_str}: No data available or an error occurred: {e}")
                # Optionally, append zero or None values for weekly data
                athlete_ids.append(athlete_id)
                names.append(name)
                week_numbers.append(week_number_str)
                date_ranges.append(f"Week {week_number_str} - No Data")
                distances.append(0.0)
                times_list.append("0h 0m")
                elevations.append(0.0)
                continue  # Proceed to the next week

    # Create DataFrame with the collected weekly data
    df_weekly = pd.DataFrame({
        'Athlete ID': athlete_ids,
        'Name': names,
        'Week Number': week_numbers,
        'Date Range': date_ranges,
        'Distance (km)': distances,
        'Time': times_list,
        'Elevation (m)': elevations
    })

    # Create DataFrame with JSON data
    df_json = pd.DataFrame(json_data_list)
    #timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    #temp_file = os.path.join(TEMP_DATA_DIR, f'temp_metadata_{timestamp}.csv')
    #df_weekly.to_csv(temp_file, index=False)
    #logging.info(f"Saved validation metadata to {temp_file}")

    return df_weekly, df_json

def process_activities(json_data, target_athlete_name):
    import pandas as pd
    import re
    from bs4 import BeautifulSoup

    # Initialize a list to store activities
    activities = []
    processed_activity_ids = set()

    # Define the clean_html function
    def clean_html(value):
        if value:
            soup = BeautifulSoup(value, 'html.parser')
            return soup.get_text(separator=' ', strip=True)
        return value

    # Define the extract_numeric function
    def extract_numeric(value):
        if value:
            value = clean_html(value)
            match = re.search(r'[\d\.]+', value)
            if match:
                return float(match.group())
        return None
    def extract_pace(value):
        if value:
            value = clean_html(value)
            # Regular expression to match formats like '6:54 /mi' or '6:54/km'
            match = re.match(r'(\d+):(\d+)\s*/\s*(mi|km)', value)
            if match:
                minutes = int(match.group(1))
                seconds = int(match.group(2))
                total_minutes = minutes + seconds / 60
                return round(total_minutes, 2)  # Round to 2 decimal places
        return None

    # Normalize the target athlete name
    target_athlete_name_normalized = target_athlete_name.strip().lower()
    print(f"Normalized target athlete name: '{target_athlete_name_normalized}'")

    # Determine where 'preFetchedEntries' is located
    if 'preFetchedEntries' in json_data:
        pre_fetched_entries = json_data['preFetchedEntries']
    elif 'appContext' in json_data and 'preFetchedEntries' in json_data['appContext']:
        pre_fetched_entries = json_data['appContext']['preFetchedEntries']
    else:
        print("Could not find 'preFetchedEntries' in json_data.")
        return pd.DataFrame()

    print(f"Number of entries in 'preFetchedEntries': {len(pre_fetched_entries)}")

    # Iterate over the entries
    for idx, entry in enumerate(pre_fetched_entries):
        print(f"\nProcessing entry {idx+1}/{len(pre_fetched_entries)}:")
        if 'activity' in entry:
            # Individual activity
            activity_data = entry.get('activity', {})
            athlete_data = activity_data.get('athlete', {})
            athlete_name = athlete_data.get('athleteName', '')
            athlete_name_normalized = athlete_name.strip().lower()
            activity_id = activity_data.get('id')
            print(f"Found individual activity by athlete: '{athlete_name}' (normalized: '{athlete_name_normalized}')")

            # Compare the normalized names
            if athlete_name_normalized != target_athlete_name_normalized:
                print(f"Skipping activity by athlete: '{athlete_name}'")
                continue
            if activity_id in processed_activity_ids:
                print(f"Skipping already processed activity {activity_id}")
                continue

            print(f"Including activity by athlete: '{athlete_name}'")
            processed_activity_ids.add(activity_id)
            # Extract stats
            stats_list = activity_data.get('stats', [])
            stats = {stat['key']: stat['value'] for stat in stats_list}

            # Build the activity dictionary
            activity_info = {
                'Athlete ID': athlete_data.get('athleteId'),
                'Athlete Name': athlete_name,
                'Activity ID': activity_data.get('id'),
                'Activity Name': activity_data.get('activityName'),
                'Description': clean_html(activity_data.get('description')),
                'Start Date': activity_data.get('startDate'),
                'Elapsed Time': activity_data.get('elapsedTime'),
                'Type': activity_data.get('type'),
                'Location': activity_data.get('timeAndLocation', {}).get('location'),
                'Stat One': stats.get('stat_one'),
                'Stat Two': stats.get('stat_two'),
                'Stat Three': stats.get('stat_three'),
                'Stat One Subtitle': stats.get('stat_one_subtitle'),
                'Stat Two Subtitle': stats.get('stat_two_subtitle'),
                'Stat Three Subtitle': stats.get('stat_three_subtitle'),
            }
            # Display the activity dictionary for debugging
            print("Activity Info (Individual):", activity_info)
            activities.append(activity_info)
        elif 'rowData' in entry and entry['rowData'].get('entity') == 'GroupActivity':
            # Group activity
            time_and_location = entry.get('timeAndLocation', {})
            location = time_and_location.get('location')

            activities_list = entry['rowData'].get('activities', [])
            print(f"Found group activity with {len(activities_list)} activities.")
            for act_idx, act in enumerate(activities_list):
                activity_id = act.get('activity_id')
                # Check if we've already processed this activity
                if activity_id in processed_activity_ids:
                    print(f"Skipping already processed activity {activity_id}")
                    continue
                # Add to processed IDs set
                processed_activity_ids.add(activity_id)
                athlete_name = act.get('athlete_name', '')
                athlete_name_normalized = athlete_name.strip().lower()
                athlete_id = act.get('athlete_id')
                print(f"Processing group activity {act_idx+1}/{len(activities_list)} for athlete: '{athlete_name}' (normalized: '{athlete_name_normalized}')")

                # Compare the normalized names
                if athlete_name_normalized != target_athlete_name_normalized:
                    print(f"Skipping group activity by athlete: '{athlete_name}'")
                    continue

                print(f"Including group activity by athlete: '{athlete_name}'")
                # Extract stats
                stats_list = act.get('stats', [])
                stats = {stat['key']: stat['value'] for stat in stats_list}

                # Build the activity dictionary
                activity_info = {
                    'Athlete ID': athlete_id,
                    'Athlete Name': athlete_name,
                    'Activity ID': act.get('activity_id'),
                    'Activity Name': act.get('name'),
                    'Description': clean_html(act.get('description')),
                    'Start Date': act.get('start_date'),
                    'Elapsed Time': act.get('elapsed_time'),
                    'Type': act.get('type'),
                    'Location': location,
                    'Stat One': stats.get('stat_one'),
                    'Stat Two': stats.get('stat_two'),
                    'Stat Three': stats.get('stat_three'),
                    'Stat One Subtitle': stats.get('stat_one_subtitle'),
                    'Stat Two Subtitle': stats.get('stat_two_subtitle'),
                    'Stat Three Subtitle': stats.get('stat_three_subtitle'),
                }
                # Display the activity dictionary for debugging
                print("Activity Info (Group):", activity_info)
                activities.append(activity_info)
        else:
            print("Entry does not contain 'activity' or 'rowData' with 'GroupActivity'.")

    print(f"\nTotal activities collected for athlete '{target_athlete_name}': {len(activities)}")
    if not activities:
        print("No activities were found for the athlete.")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(activities)

    # Further processing (e.g., extracting numeric values)
    df['Distance (km)'] = df['Stat One'].apply(extract_numeric)
    df['Pace (min/km)'] = df['Stat Two'].apply(extract_pace)
    df['Time'] = df['Stat Three'].apply(clean_html)

    # Convert 'Start Date' to datetime
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')

    # Drop the original stat columns
    df.drop(['Stat One', 'Stat Two', 'Stat Three',
             'Stat One Subtitle', 'Stat Two Subtitle', 'Stat Three Subtitle'], axis=1, inplace=True)

    return df
