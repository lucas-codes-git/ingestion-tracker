from src.services.clients import TicketMasterClient, SupaBaseClient
from src.services.tracker import IngestionTracker
from src.services.utils import build_batch_id, FileExtensions

from hashlib import sha256
import json
import logging


ticketmasterclient = TicketMasterClient()
supaclient = SupaBaseClient()

logger = logging.getLogger(__name__)

ENDPOINT = "events"


async def run_test(tracker: IngestionTracker):

    ingestion_id = None

    try:
        data = await ticketmasterclient.fetch_data(
            endpoint=ENDPOINT,
            params={
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

        file_path = (
            f"ticketmaster/"
            f"{ENDPOINT}/"
            f"raw/"
            f"{batch_id}"
        )

        ingestion_id = tracker.insert_job(
            source_system="ticketmaster",
            source_url=ticketmasterclient.build_url(endpoint=ENDPOINT),
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


        bronze_started = tracker.start_bronze_job(
            ingestion_id
        )

        if not bronze_started:
            logger.error(
                "Could not start bronze job: %s",
                batch_id
            )
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


        bronze_completed = tracker.complete_bronze_job(
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