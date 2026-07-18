from src.services.tracker.ingestion_tracker import IngestionTracker

def fetch_silver_file() -> list[dict]:
    tracker = IngestionTracker()
    files = tracker.fetch_pending_silver_files()
    
    silver_files = []
    
    for file in files:
        silver_files.append(
            {
                "ingestion_id": file["ingestion_id"],
                "batch_id": file["batch_id"],
                "file_path": file["file_path"]
            }
        )
        
    return silver_files