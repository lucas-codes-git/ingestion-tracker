from src.services.clients import TicketMasterClient, SupaBaseClient
from src.services.tracker import IngestionTracker
import asyncio
from src.services.utils import build_batch_id
from src.services.utils import FileExtensions
from hashlib import sha256
import json
import logging

ticketmasterclient = TicketMasterClient()
supaclient = SupaBaseClient()
tracker = IngestionTracker()

logger = logging.getLogger(__name__)

ENDPOINT = "events"

async def run_test():
    
    tracker.create_ingestion_tracker_table()
    ingestion_id = None
    
    try:
        data = await ticketmasterclient.fetch_data(
            endpoint=ENDPOINT,
            params = {
                "city": "Toronto",
                "classificationName": "sports",
                "countryCode": "CA",
                "size": 10
            }
        )
    
        formatted_json = json.dumps(data).encode("utf-8")

        content_hash = sha256(formatted_json).hexdigest()
        bytes_size = len(formatted_json)
        
        batch_id = build_batch_id(
            source="ticketmaster",
            endpoint=ENDPOINT,
            file_hash=content_hash,
            extension=FileExtensions.JSON.extension
        )
        ingestion_id = tracker.insert_job(
            source_system="ticketmaster",
            source_url=ticketmasterclient.build_url(endpoint=ENDPOINT),
            file_type=FileExtensions.JSON.extension,
            bytes_size=bytes_size,
            content_hash=content_hash,
            batch_id=batch_id
        )
        
        if ingestion_id is None:
            logger.warning("File was already ingested, skipping file")
            return
        
        is_processed = tracker.start_processing(ingestion_id)
        if not is_processed:
            logger.error(f"Could not start processing file: {batch_id}")
            return

        supaclient.upload_file(
            bucket_name="lucas-infra",
            source_name="ticketmaster",
            data_name=ENDPOINT,
            clean_raw=False,
            file_name=batch_id,
            file_bytes=formatted_json,
            file_type="application/json"
        )
        
        completed_job = tracker.complete_job(ingestion_id)
        if completed_job:
            logger.info(f"Successfully ingested: {batch_id}")
        else:
            logger.error(f"Could not complete ingestion for: {batch_id}")
        
    except Exception as e:
        if ingestion_id:
            tracker.fail_job(ingestion_id, str(e))
            
        raise
    
    finally:
        await ticketmasterclient.close()

if __name__ == "__main__":
    asyncio.run(run_test())