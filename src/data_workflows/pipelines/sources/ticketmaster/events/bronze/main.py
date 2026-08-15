from src.services.clients import TicketMasterClient, SupaBaseClient
from src.services.tracker import IngestionTracker
from src.services.utils import build_batch_id, FileExtensions
from src.data_workflows.pipelines.sources.ticketmaster.events.bronze import BRONZE_EVENTS_CONFIG
from src.data_workflows.pipelines.common import retry_bronze_job
from src.services.utils import fetch_secrets

from hashlib import sha256
import json
import logging

secrets = fetch_secrets()

logger = logging.getLogger(__name__)

config = BRONZE_EVENTS_CONFIG


async def run_test(
    tracker: IngestionTracker,
    ticketmaster: TicketMasterClient,
    supabase: SupaBaseClient
):

    retry_files = await tracker.retry_failed_bronze_files(endpoint=config["endpoint"])
    
    for file in retry_files:
        await retry_bronze_job(
            tracker=tracker,
            supabase=supabase,
            file=file,
            bucket_name=secrets["bucket_name"]
        )
    
    ingestion_id = None

    try:
        data = await ticketmaster.fetch_data(
            endpoint=config["endpoint"],
            params={
                "city": config["city"],
                "countryCode": config["countryCode"],
                "size": config["size"]
            }
        )

        formatted_json = json.dumps(data).encode("utf-8")

        content_hash = sha256(formatted_json).hexdigest()
        bytes_size = len(formatted_json)

        batch_id = build_batch_id(
            source=config["source"],
            endpoint=config["endpoint"],
            file_hash=content_hash,
            extension=FileExtensions.JSON.extension
        )

        file_path = (
            f"{config["source"]}/"
            f"{config["endpoint"]}/"
            f"raw/"
            f"{batch_id}"
        )

        ingestion_id = await tracker.insert_job(
            source_system=config["source"],
            source_url=ticketmaster.build_url(endpoint=config["endpoint"]),
            endpoint=config["endpoint"],
            file_type=FileExtensions.JSON.extension,
            bytes_size=bytes_size,
            content_hash=content_hash,
            batch_id=batch_id,
            file_path=file_path
        )

        if ingestion_id is None:
            logger.warning(
                "File already ingested, skipping: %s",
                batch_id
            )
            return


        bronze_started = await tracker.start_bronze_job(
            ingestion_id
        )

        if not bronze_started:
            logger.error(
                "Could not start bronze job: %s",
                batch_id
            )
            return


        supabase.upload_file(
            bucket_name=secrets["bucket_name"],
            source_name=config["source"],
            data_name=config["endpoint"],
            clean_raw=False,
            file_name=batch_id,
            file_bytes=formatted_json,
            file_type=FileExtensions.JSON.content_type()
        )


        bronze_completed = await tracker.complete_bronze_job(
            ingestion_id
        )

        if bronze_completed:
            logger.info(
                "Successfully completed bronze ingestion: %s",
                batch_id
            )
        else:
            logger.error(
                "Could not complete bronze ingestion: %s",
                batch_id
            )


    except Exception as e:

        logger.exception(
            "Bronze ingestion failed"
        )

        if ingestion_id:
            tracker.fail_bronze(
                ingestion_id,
                str(e)
            )

        raise