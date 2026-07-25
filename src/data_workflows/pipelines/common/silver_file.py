from src.services.tracker import IngestionTracker
from src.services.clients import SupaBaseClient
from src.services.utils import fetch_secrets

async def prep_silver_files(
        endpoint: str,
        tracker: IngestionTracker,
        supa: SupaBaseClient
    ) -> list[dict]:
    secrets = fetch_secrets()
    
    files = await tracker.fetch_pending_silver_files()
    
    silver_files = []
    
    for file in files:
        if file["endpoint"] == endpoint.lower():
            path = file["file_path"]
            file_bytes = supa.download_file(
                bucket_name=secrets["bucket_name"],
                file_path=path
            )
            
            silver_files.append(
                {
                    "ingestion_id": file["ingestion_id"],
                    "batch_id": file["batch_id"],
                    "file_path": file["file_path"],
                    "content": file_bytes
                }
            )
            
    return silver_files
    
    