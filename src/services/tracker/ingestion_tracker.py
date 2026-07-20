from src.services.database import pool
from src.services.utils import JobStatus


class IngestionTracker:
    def __init__(self):
        self.STATUS = JobStatus
        
    async def create_ingestion_tracker_table(self) -> None:
        async with pool.connection() as conn:
             async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE EXTENSION IF NOT EXISTS pgcrypto;
                """)

                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_tracker (
                        ingestion_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        source_system TEXT NOT NULL,
                        file_type TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        endpoint TEXT NOT NULL,
                        bytes_size BIGINT NOT NULL,
                        content_hash TEXT NOT NULL UNIQUE,
                        batch_id TEXT NOT NULL UNIQUE,
                        file_path TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        bronze_status TEXT NOT NULL DEFAULT 'pending',
                        silver_status TEXT NOT NULL DEFAULT 'pending',
                        bronze_started_at TIMESTAMPTZ,
                        bronze_finished_at TIMESTAMPTZ,
                        silver_started_at TIMESTAMPTZ,
                        silver_finished_at TIMESTAMPTZ,
                        bronze_run_duration_seconds BIGINT,
                        silver_run_duration_seconds BIGINT,
                        error_msg TEXT
                    );
                """)
             await conn.commit()

    async def insert_job(
            self,
            source_system: str,
            source_url: str,
            endpoint: str,
            file_type: str,
            bytes_size: int,
            content_hash: str,
            batch_id: str,
            file_path: str
        ) -> str | None:

        async with pool.connection() as conn:
            async with conn.cursor() as cur:

                query = """
                INSERT INTO ingestion_tracker (
                    source_system,
                    source_url,
                    endpoint,
                    file_type,
                    bytes_size,
                    content_hash,
                    batch_id,
                    file_path
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_hash) DO NOTHING
                RETURNING ingestion_id;
                """

                await cur.execute(
                    query,
                    (
                        source_system,
                        source_url,
                        endpoint,
                        file_type,
                        bytes_size,
                        content_hash,
                        batch_id,
                        file_path
                    )
                )

                inserted = await cur.fetchone()
                await conn.commit()

                if inserted is None:
                    return None

                return inserted[0]
    
    async def retry_failed_bronze_files(self, endpoint: str) -> list[dict]:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        error_msg = NULL,
                        bronze_started_at = NULL,
                        bronze_finished_at = NULL,
                        bronze_run_duration_seconds = NULL
                    WHERE endpoint = %s
                    AND bronze_status = %s
                    RETURNING
                        ingestion_id,
                        batch_id,
                        file_path,
                        endpoint
                """
                
                await cur.execute(query,(JobStatus.PENDING.value, endpoint, JobStatus.FAILED.value))
                rows = await cur.fetchall()
                
            await conn.commit()
                
        files = []
        for row in rows:
            files.append(
                {
                    "ingestion_id": row[0],
                    "batch_id": row[1],
                    "file_path": row[2],
                    "endpoint": row[3]
                }
            )
        return files
                
    async def fetch_pending_bronze_files(self) -> list[tuple]:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT
                        ingestion_id,
                        batch_id,
                        file_path,
                        endpoint
                    FROM ingestion_tracker
                    WHERE bronze_status = %s
                    AND silver_status = %s
                """
                await cur.execute(
                    query,(
                        JobStatus.PENDING.value, JobStatus.PENDING.value
                    )
                )
                
                return await cur.fetchall()
                
                
    async def fetch_pending_silver_files(self) -> list[dict]:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT
                        ingestion_id,
                        batch_id,
                        file_path,
                        endpoint
                    FROM ingestion_tracker
                    WHERE bronze_status = %s
                    AND silver_status = %s
                """
                await cur.execute(
                    query,(
                        JobStatus.COMPLETED.value, JobStatus.PENDING.value
                    )
                )
                
                rows = await cur.fetchall()
                
                files = []
                
                for row in rows:
                    file_record =  {
                        "ingestion_id": row[0],
                        "batch_id": row[1],
                        "file_path": row[2],
                        "endpoint": row[3]
                    }
                
                    files.append(file_record)
                
                return files
        
#               Bronze methods

    async def start_bronze_job(self, ingestion_id: str) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        bronze_started_at = NOW()
                    WHERE ingestion_id = %s
                    AND bronze_status = %s
                """
                
                await cur.execute(
                    query,
                    (
                        JobStatus.PROCESSING.value,
                        ingestion_id,
                        JobStatus.PENDING.value
                    )
                )
                await conn.commit()
                if cur.rowcount == 0:
                    return False
                return True


    async def complete_bronze_job(self, ingestion_id: str) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        bronze_finished_at = NOW(),
                        bronze_run_duration_seconds = 
                        EXTRACT(
                            EPOCH FROM
                            (NOW() - bronze_started_at)
                        )
                    WHERE ingestion_id = %s
                    AND bronze_status = %s
                """
            
                await cur.execute(
                    query,(
                        JobStatus.COMPLETED.value,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                await conn.commit()

                if cur.rowcount == 0:
                    return False
                return True

    async def fail_bronze(
        self,
        ingestion_id: str,
        error_msg: str
    ) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        error_msg = %s
                    WHERE ingestion_id = %s
                    AND bronze_status = %s
                """
                
                await cur.execute(
                    query,(
                        JobStatus.FAILED.value,
                        error_msg,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                await conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True



#                   Silver methods

    async def start_silver(self, ingestion_id: str) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        silver_status = %s,
                        silver_started_at = NOW()
                    WHERE ingestion_id = %s
                    AND silver_status = %s
                """
                
                await cur.execute(
                    query,(
                        JobStatus.PROCESSING.value,
                        ingestion_id,
                        JobStatus.PENDING.value
                    )
                )
                
                await conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True


    async def complete_silver(self, ingestion_id: str) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        silver_status = %s,
                        silver_finished_at = NOW(),
                        silver_run_duration_seconds = 
                        EXTRACT(
                            EPOCH FROM
                            (NOW() - silver_started_at)
                        )
                    WHERE ingestion_id = %s
                    AND silver_status = %s
                """
                
                await cur.execute(
                    query,(
                        JobStatus.COMPLETED.value,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                await conn.commit()
            
                if cur.rowcount == 0:
                    return False
                return True

    async def fail_silver(
        self,
        ingestion_id: str,
        error_msg: str
    ) -> bool:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        silver_status = %s,
                        error_msg = %s
                    WHERE ingestion_id = %s
                    AND silver_status = %s
                """
            
                await cur.execute(
                    query,(
                        JobStatus.FAILED.value,
                        error_msg,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                await conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True