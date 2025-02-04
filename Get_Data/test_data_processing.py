import pandas as pd
import numpy as np
import logging
from pathlib import Path
from data_processing import (
    time_to_minutes,
    clean_activities_data,
    calculate_athlete_metrics,
    update_athlete_metadata,
    update_databases,
    process_data
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestDataProcessing:
    """Test suite for data_processing.py"""
    
    def __init__(self):
        """Initialize test data paths"""
        self.test_data_dir = Path("../data/tempdata")
        self.metadata_dir = Path("../data/metadata")
        self.test_files = {
            'raw_activities': self.test_data_dir / "processed_AdamFOGG_to_ThomasBRIDGER_w45-47_20250204_125324.csv",
            'master_db': self.metadata_dir / "master_iaaf_database_with_strava.csv",
            'metadata': Path("../cleaned_athlete_metadata.csv"),
            'activities_db': Path("../indiv_activities_full.csv")
        }
        print("\nFiles that will be accessed:")
        print("-" * 50)
        for name, path in self.test_files.items():
            print(f"{name:15} | {path}")
        
        self.file_operations = {
            'read': set(),
            'write': set()
        }
        # Test athletes
        self.test_athlete_ids = [45537525, 4814818, 4928335]  # Adam FOGG, JP FLAVIN, Thomas BRIDGER
    def track_operation(self, file_path: Path, operation: str):
        """Track file operations"""
        self.file_operations[operation].add(str(file_path))
    def print_file_operations(self):
        """Print summary of file operations"""
        print("\nFile Operations Summary:")
        print("-" * 50)
        print("\nFiles Read:")
        for file in sorted(self.file_operations['read']):
            print(f"- {file}")
        print("\nFiles Modified:")
        for file in sorted(self.file_operations['write']):
            print(f"- {file}")
    def test_time_to_minutes(self):
        """Test time string conversion"""
        test_cases = {
            "1h 30m": 90,
            "45m": 45,
            "30s": 0.5,
            "2h 15m 30s": 135.5,
            "invalid": 0
        }
        
        print("\nTesting time_to_minutes function:")
        for time_str, expected in test_cases.items():
            result = time_to_minutes(time_str)
            print(f"Input: {time_str:10} | Expected: {expected:6} | Got: {result:6} | {'✓' if abs(result - expected) < 0.01 else '✗'}")
    
    def test_clean_activities(self):
        """Test clean_activities_data function"""
        try:
            # Load test data
            activities_df = pd.read_csv(self.test_files['raw_activities'])
            self.track_operation(self.test_files['raw_activities'], 'read')
            master_df = pd.read_csv(self.test_files['master_db'])
            self.track_operation(self.test_files['master_db'], 'read')
            # Get target athletes
            target_athletes = master_df[master_df['Athlete ID'].isin(self.test_athlete_ids)]['Competitor'].tolist()
            
            print("\nTesting clean_activities_data:")
            print(f"Input shape: {activities_df.shape}")
            
            # Clean data
            clean_df = clean_activities_data(activities_df, target_athletes)
            
            if clean_df is not None and not clean_df.empty:
                print("\nCleaning successful:")
                print(f"Output shape: {clean_df.shape}")
                print("\nData types:")
                for col in clean_df.columns:
                    print(f"{col:25} | {clean_df[col].dtype}")
                print("\nSample data:")
                print(clean_df.head())
                return clean_df
            else:
                print("Error: Cleaning failed")
                return None
                
        except Exception as e:
            print(f"Error in test_clean_activities: {str(e)}")
            return None
    
    def test_calculate_metrics(self):
        """Test calculate_athlete_metrics function"""
        try:
            # Get clean activities
            clean_df = self.test_clean_activities()
            if clean_df is None:
                return
                
            # Get target athletes
            master_df = pd.read_csv(self.test_files['master_db'])
            self.track_operation(self.test_files['metadata'], 'read')
            target_athletes = master_df[master_df['Athlete ID'].isin(self.test_athlete_ids)]['Competitor'].tolist()
            
            print("\nTesting calculate_athlete_metrics:")
            metrics_df = calculate_athlete_metrics(clean_df, target_athletes)
            
            if not metrics_df.empty:
                print("\nMetrics calculation successful:")
                print(f"Output shape: {metrics_df.shape}")
                print("\nMetrics per athlete:")
                for athlete in metrics_df['Athlete Name'].unique():
                    print(f"\n{athlete}:")
                    athlete_metrics = metrics_df[metrics_df['Athlete Name'] == athlete]
                    print(athlete_metrics.to_string(index=False))
                return metrics_df
            else:
                print("Error: Metrics calculation failed")
                return None
                
        except Exception as e:
            print(f"Error in test_calculate_metrics: {str(e)}")
            return None
    
    def test_update_metadata(self):
        """Test update_athlete_metadata function"""
        try:
            # Get metrics
            metrics_df = self.test_calculate_metrics()
            if metrics_df is None:
                return
                
            # Load current metadata
            metadata_df = pd.read_csv(self.test_files['metadata'])
            
            print("\nTesting update_athlete_metadata:")
            updated_df = update_athlete_metadata(metadata_df, metrics_df, 45, 47)
            self.track_operation(self.test_files['metadata'], 'read')
            if updated_df is not None:
                self.track_operation(self.test_files['metadata'], 'write')
            if not updated_df.empty:
                print("\nMetadata update successful:")
                print(f"Output shape: {updated_df.shape}")
                print("\nUpdated metrics for test athletes:")
                test_athletes = updated_df[updated_df['Athlete ID'].isin(self.test_athlete_ids)]
                print(test_athletes.to_string(index=False))
                return updated_df
            else:
                print("Error: Metadata update failed")
                return None
                
        except Exception as e:
            print(f"Error in test_update_metadata: {str(e)}")
            return None
    
    def test_full_pipeline(self):
        """Test entire data processing pipeline"""
        try:
            print("\nTesting full data processing pipeline:")
            # Track all potential file operations in the pipeline
            self.track_operation(self.test_files['raw_activities'], 'read')
            self.track_operation(self.test_files['master_db'], 'read')
            self.track_operation(self.test_files['metadata'], 'read')
            self.track_operation(self.test_files['activities_db'], 'read')
            
            self.track_operation(self.test_files['metadata'], 'write')
            self.track_operation(self.test_files['activities_db'], 'write')
            self.track_operation(self.test_files['master_db'], 'write')
            
            success = process_data(45, 47)
            
            if success:
                print("Pipeline completed successfully")
            else:
                print("Pipeline failed")
                
        except Exception as e:
            print(f"Error in test_full_pipeline: {str(e)}")

def main():
    """Run all tests"""
    test_suite = TestDataProcessing()
    
    # Run individual function tests
    test_suite.test_time_to_minutes()
    test_suite.test_clean_activities()
    test_suite.test_calculate_metrics()
    test_suite.test_update_metadata()
    
    # Run full pipeline test
    test_suite.test_full_pipeline()

if __name__ == "__main__":
    main()