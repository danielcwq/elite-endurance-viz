import pandas as pd
import numpy as np

def add_athlete_ids_to_metadata():
    """Add Athlete IDs from master database to cleaned_athlete_metadata"""
    try:
        # Read both files
        metadata_df = pd.read_csv("cleaned_athlete_metadata.csv")
        master_df = pd.read_csv("data/metadata/master_iaaf_database_with_strava.csv")
        def normalize_name(name):
            """Normalize athlete names consistently"""
            if not isinstance(name, str):
                return name
            name = name.strip()
            # Special cases
            if "jp" in name.lower():
                return name.upper()  # "JP FLAVIN"
            # Handle other special cases (e.g., McNames)
            parts = name.split()
            return " ".join(part.upper() for part in parts)
        metadata_df['Athlete Name'] = metadata_df['Athlete Name'].apply(normalize_name)
        master_df['Competitor'] = master_df['Competitor'].apply(normalize_name)
        
        # Print before mapping to check names
        print("\nSample of normalized names:")
        print("\nMetadata names:")
        print(metadata_df['Athlete Name'].unique())
        print("\nMaster database names:")
        print(master_df['Competitor'].unique())
        
        # Create name to ID mapping from master database
        id_mapping = master_df[['Competitor', 'Athlete ID']].dropna()
        id_mapping = id_mapping.set_index('Competitor')['Athlete ID'].to_dict()
        print("\nID Mapping:")
        for name, id in id_mapping.items():
            print(f"{name}: {id}")
        # Add Athlete ID column to metadata
        metadata_df['Athlete ID'] = metadata_df['Athlete Name'].map(id_mapping)
        unmatched = metadata_df[metadata_df['Athlete ID'].isna()]
        if not unmatched.empty:
            print("\nWarning: Unmatched names found:")
            for name in unmatched['Athlete Name'].unique():
                print(f"- {name}")
        # Fill missing IDs with 0
        metadata_df['Athlete ID'] = metadata_df['Athlete ID'].fillna(0).astype('int64')
        
        # Save updated metadata
        metadata_df.to_csv("cleaned_athlete_metadata.csv", index=False)
        print(f"Added Athlete IDs to {len(metadata_df)} records")
        
        # Show statistics
        print(f"\nStatistics:")
        print(f"Total records: {len(metadata_df)}")
        print(f"Records with valid IDs: {(metadata_df['Athlete ID'] > 0).sum()}")
        print(f"Records without IDs: {(metadata_df['Athlete ID'] == 0).sum()}")
        
        return metadata_df
    except Exception as e:
        print(f"Error adding athlete IDs: {str(e)}")
        return None
    
add_athlete_ids_to_metadata()