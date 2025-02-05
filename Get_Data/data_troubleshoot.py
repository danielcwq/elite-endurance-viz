import logging
from datetime import datetime
import os
import sys
from data_processing import calculate_athlete_metrics, update_metadata_databases

# Setup logging
logging.basicConfig(
    filename='../logs/automation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def recalculate_metrics(target_ids: list, start_week: int, end_week: int) -> bool:
    """Recalculate metrics and update metadata without collecting new data
    
    Args:
        target_ids: List of athlete IDs to recalculate
        start_week: Starting week number
        end_week: Current week number for average calculations
    """
    logger.info(f"Recalculating metrics for athletes: {target_ids}")
    
    try:
        # Calculate new metrics using existing activities
        new_metrics = calculate_athlete_metrics(target_ids, end_week)
        if new_metrics.empty:
            logger.error("Failed to calculate new metrics")
            return False
        
        # Update metadata databases with new calculations
        success = update_metadata_databases(new_metrics, start_week, end_week)
        if success:
            logger.info("Successfully updated metadata with recalculated metrics")
            return True
            
        logger.error("Failed to update metadata databases")
        return False
        
    except Exception as e:
        logger.error(f"Error in recalculate_metrics: {str(e)}")
        return False
# Example usage
target_ids = [4814818]  # Athlete to recalculate
success = recalculate_metrics(
    target_ids=target_ids,
    start_week=45,
    end_week=52
)