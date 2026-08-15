import polars as pl
from src.services.tracker import IngestionTracker
from src.services.clients import SupaBaseClient
from src.services.utils.extensions import FileExtensions
from src.data_workflows.pipelines.common import prep_silver_files, load_file
from src.data_workflows.pipelines.sources.ticketmaster.events.silver import silver_events_transformations
import logging
from psycopg import sql
from src.services.database import pool
from src.data_workflows.pipelines.common.sql.writes.create_table import create_table
import polars as pl

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
            # Write dataframe to Postgres (simple upsert by event_id)
            target_table = "ticketmaster_events"

            cols = df.columns

            # Ensure target table exists. Map polars dtypes to SQL types.
            cols_dtype: dict[str, str] = {}
            for col, dtype in zip(cols, df.dtypes):
                t = str(dtype).lower()
                if "utf" in t or "str" in t or "object" in t:
                    sql_type = "TEXT"
                elif "bool" in t:
                    sql_type = "BOOLEAN"
                elif "datetime" in t:
                    sql_type = "TIMESTAMPTZ"
                elif "date" in t:
                    sql_type = "DATE"
                elif "time" in t:
                    sql_type = "TIME"
                else:
                    sql_type = "TEXT"

                if col == "event_id":
                    sql_type += " PRIMARY KEY"

                cols_dtype[col] = sql_type

            # create table if not exists
            create_q = create_table(target_table, cols_dtype)
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(create_q)
                await conn.commit()

            columns_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
            placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in cols)

            update_cols = [c for c in cols if c != "event_id"]
            updates_sql = sql.SQL(', ').join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in update_cols
            ) if update_cols else sql.SQL('')

            insert_sql = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({values})
                ON CONFLICT (event_id)
                DO UPDATE SET {updates};
            """).format(
                table=sql.Identifier(target_table),
                columns=columns_sql,
                values=placeholders,
                updates=updates_sql
            )

            # Avoid using `to_numpy()` (requires numpy). Use `to_dicts()` to
            # build tuples in column order which works with polars alone.
            rows = []
            if len(df) > 0:
                dicts = df.to_dicts()
                for d in dicts:
                    rows.append(tuple(d.get(c) for c in cols))

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    for row in rows:
                        await cur.execute(insert_sql, row)
                await conn.commit()

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
        
        