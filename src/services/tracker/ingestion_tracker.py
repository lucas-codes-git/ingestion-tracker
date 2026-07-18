from psycopg_pool import ConnectionPool
from src.services.utils import fetch_secrets
from src.services.utils import JobStatus


class IngestionTracker:
    def __init__(self):
        self.env = fetch_secrets()
        self.pool = ConnectionPool(
            conninfo=self.env["supadburl"],
            min_size=1,
            max_size=2,
            open=False
        )
        self.STATUS = JobStatus
        
    def start(self):
        self.pool.open()
        self.pool.wait()
        
    def close(self):
        self.pool.close()
        
    def create_ingestion_tracker_table(self) -> None:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE EXTENSION IF NOT EXISTS pgcrypto;
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ingestion_tracker (
                        ingestion_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        source_system TEXT NOT NULL,
                        file_type TEXT NOT NULL,
                        source_url TEXT NOT NULL,
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
            conn.commit()

    def insert_job(
            self,
            source_system: str,
            source_url: str,
            file_type: str,
            bytes_size: int,
            content_hash: str,
            batch_id: str,
            file_path: str
        ) -> str | None:

        with self.pool.connection() as conn:
            with conn.cursor() as cur:

                query = """
                INSERT INTO ingestion_tracker (
                    source_system,
                    source_url,
                    file_type,
                    bytes_size,
                    content_hash,
                    batch_id,
                    file_path
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_hash) DO NOTHING
                RETURNING ingestion_id;
                """

                cur.execute(
                    query,
                    (
                        source_system,
                        source_url,
                        file_type,
                        bytes_size,
                        content_hash,
                        batch_id,
                        file_path
                    )
                )

                inserted = cur.fetchone()
                conn.commit()

                if inserted is None:
                    return None

                return inserted[0]

    def fetch_pending_bronze_files(self) -> list[tuple]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT
                        ingestion_id,
                        batch_id,
                        file_path
                    FROM ingestion_tracker
                    WHERE bronze_status = %s
                    AND silver_status = %s
                """
                cur.execute(
                    query,(
                        JobStatus.PENDING.value, JobStatus.PENDING.value
                    )
                )
                
                return cur.fetchall()
                
                
    def fetch_pending_silver_files(self) -> list[dict]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT
                        ingestion_id,
                        batch_id,
                        file_path
                    FROM ingestion_tracker
                    WHERE bronze_status = %s
                    AND silver_status = %s
                """
                cur.execute(
                    query,(
                        JobStatus.COMPLETED.value, JobStatus.PENDING.value
                    )
                )
                
                rows = cur.fetchall()
                
                files = []
                
                for row in rows:
                    file_record =  {
                        "ingestion_id": row[0],
                        "batch_id": row[1],
                        "file_path": row[2]
                    }
                
                    files.append(file_record)
                
                return files
        
#               Bronze methods

    def start_bronze_job(self, ingestion_id: str) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        bronze_started_at = NOW()
                    WHERE ingestion_id = %s
                    AND bronze_status = %s
                """
                
                cur.execute(
                    query,
                    (
                        JobStatus.PROCESSING.value,
                        ingestion_id,
                        JobStatus.PENDING.value
                    )
                )
                conn.commit()
                if cur.rowcount == 0:
                    return False
                return True


    def complete_bronze_job(self, ingestion_id: str) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
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
            
                cur.execute(
                    query,(
                        JobStatus.COMPLETED.value,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                conn.commit()

                if cur.rowcount == 0:
                    return False
                return True

    def fail_bronze(
        self,
        ingestion_id: str,
        error_msg: str
    ) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        bronze_status = %s,
                        error_msg = %s
                    WHERE ingestion_id = %s
                    AND bronze_status = %s
                """
                
                cur.execute(
                    query,(
                        JobStatus.FAILED.value,
                        error_msg,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True



#                   Silver methods

    def start_silver(self, ingestion_id: str) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        silver_status = %s,
                        silver_started_at = NOW()
                    WHERE ingestion_id = %s
                    AND silver_status = %s
                """
                
                cur.execute(
                    query,(
                        JobStatus.PROCESSING.value,
                        ingestion_id,
                        JobStatus.PENDING.value
                    )
                )
                
                conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True


    def complete_silver(self, ingestion_id: str) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
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
                
                cur.execute(
                    query,(
                        JobStatus.COMPLETED.value,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                conn.commit()
            
                if cur.rowcount == 0:
                    return False
                return True

    def fail_silver(
        self,
        ingestion_id: str,
        error_msg: str
    ) -> bool:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                
                query = """
                    UPDATE ingestion_tracker
                    SET
                        silver_status = %s,
                        error_msg = %s
                    WHERE ingestion_id = %s
                    AND silver_status = %s
                """
            
                cur.execute(
                    query,(
                        JobStatus.FAILED.value,
                        error_msg,
                        ingestion_id,
                        JobStatus.PROCESSING.value
                    )
                )
                
                conn.commit()
                
                if cur.rowcount == 0:
                    return False
                return True