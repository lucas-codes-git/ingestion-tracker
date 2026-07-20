import polars as pl
from src.data_workflows.pipelines.common import load_file
from src.services.tracker import IngestionTracker

def silver_ticketmaster_events() -> pl.DataFrame:
    
    tracker = IngestionTracker()
    
    files = tracker.fetch_pending_silver_files(
        
    )