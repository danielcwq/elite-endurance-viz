import pandas as pd
def collect_strava_data(start_week: int, end_week: int, specific_ids: list = None) -> pd.DataFrame:
    """Collect new Strava data for specified weeks and athletes
    
    Returns:
        pd.DataFrame: new_activities_df
    """
    from test_scraping import check_data_updates
    
    new_activities, _ = check_data_updates(
        start_week=start_week,
        end_week=end_week,
        specific_ids=specific_ids
    )
    
    return new_activities