import polars as pl
from src.services.tracker import IngestionTracker
from src.services.clients import SupaBaseClient
from src.services.utils.extensions import FileExtensions
from src.data_workflows.pipelines.common import prep_silver_files, load_file
from src.data_workflows.pipelines.sources.ticketmaster.events.silver import silver_events_transformations
import logging

logger = logging.getLogger(__name__)

async def run_silver_ticketmaster_events(
    tracker: IngestionTracker,
    supa: SupaBaseClient,
) -> None:

    files = await prep_silver_files(
        endpoint="events",
        tracker=tracker,
        supa=supa,
    )

    for file in files:

        logger.info("Starting silver transformation for %s", file["batch_id"])
        
        try:

            await tracker.start_silver(
                ingestion_id=file["ingestion_id"]
            )
            
            logger.info("Loading %s", file["batch_id"])

            df = load_file(
                file_bytes=file["content"],
                file_type=FileExtensions.JSON,
            )
            
            logger.info("Transforming %s", file["batch_id"])

            df = silver_events_transformations(df)
            
            logger.info(
            "Successfully transformed %s (%d rows)",
            file["batch_id"],
            len(df)
            )

            logger.info("Writing %s to Postgres", file["batch_id"])
            
            await tracker.complete_silver(
                ingestion_id=file["ingestion_id"]
            )

        except Exception as e:
            
            logger.exception(
            "Failed processing %s",
            file["batch_id"]
        )
            
            await tracker.fail_silver(
                ingestion_id=file["ingestion_id"],
                error_msg=str(e),
            )
        
        