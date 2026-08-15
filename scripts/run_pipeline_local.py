import asyncio
import json
import sys
import types
from hashlib import sha256

# Provide a fake `src.services.clients` module so importing pipeline modules
# doesn't require installing external client libs in this local test.
mock_clients = types.ModuleType("src.services.clients")
mock_clients.TicketMasterClient = object
mock_clients.SupaBaseClient = object
sys.modules["src.services.clients"] = mock_clients

# Also mock src.services.tracker to avoid importing DB-backed tracker implementation
mock_tracker_mod = types.ModuleType("src.services.tracker")
class DummyTracker:
    pass
mock_tracker_mod.IngestionTracker = DummyTracker
sys.modules["src.services.tracker"] = mock_tracker_mod

from src.data_workflows.pipelines.sources.ticketmaster.events.bronze import main as bronze_main
import importlib.util
from pathlib import Path

# Load transformations module directly to avoid package-level imports
package_name = "src.data_workflows.pipelines.sources.ticketmaster.events.silver"

# Load schema module into sys.modules so relative import works
schema_path = Path("src/data_workflows/pipelines/sources/ticketmaster/events/silver/schema.py")
schema_spec = importlib.util.spec_from_file_location(f"{package_name}.schema", schema_path)
schema_mod = importlib.util.module_from_spec(schema_spec)
schema_spec.loader.exec_module(schema_mod)
sys.modules[f"{package_name}.schema"] = schema_mod

# Now load transformations as part of the package
trans_path = Path("src/data_workflows/pipelines/sources/ticketmaster/events/silver/transformations.py")
spec = importlib.util.spec_from_file_location(f"{package_name}.transformations", trans_path)
silver_transform = importlib.util.module_from_spec(spec)
silver_transform.__package__ = package_name
spec.loader.exec_module(silver_transform)
sys.modules[f"{package_name}.transformations"] = silver_transform

from src.services.utils import FileExtensions

class MockTicketMasterClient:
    def __init__(self):
        pass

    async def fetch_data(self, endpoint: str, params: dict):
        # Return a simplified Ticketmaster payload with _embedded.events
        return {
            "_embedded": {
                "events": [
                    {
                        "id": "evt1",
                        "name": "Test Event",
                        "type": "event",
                        "url": "http://example.com/event",
                        "locale": "en-US",
                        "_embedded": {
                                    "venues": [{"id": "venue1"}],
                                    "attractions": [{"id": "attr1"}]
                                },
                        "promoter": {"id": "prom1"},
                        "classifications": [
                            {
                                "segment": {"id": "seg1"},
                                "genre": {"id": "g1"},
                                "subGenre": {"id": "sg1"},
                                "type": {"id": "t1"},
                                "subType": {"id": "st1"}
                            }
                        ],
                        "dates": {
                            "start": {
                                "localDate": "2026-08-14",
                                "localTime": "20:00",
                                "dateTime": "2026-08-14T20:00:00Z",
                                "timezone": "UTC",
                                "dateTBD": False,
                                "dateTBA": False,
                                "timeTBA": False,
                                "noSpecificTime": False
                            },
                            "status": {"code": "onsale"},
                            "spanMultipleDays": False
                        }
                    }
                ]
            }
        }

    def build_url(self, endpoint: str):
        return f"https://mock/{endpoint}"

    async def close(self):
        return None

class MockSupaBaseClient:
    def __init__(self):
        self.storage = {}

    def upload_file(self, bucket_name: str, source_name: str, data_name: str, clean_raw: bool, file_name: str, file_bytes: bytes, file_type: str = "application/json"):
        path = f"{source_name}/{data_name}/raw/{file_name}"
        self.storage[path] = file_bytes

    def download_file(self, bucket_name: str, file_path: str) -> bytes:
        return self.storage.get(file_path, b"")

class MockTracker:
    def __init__(self):
        self.jobs = {}

    async def retry_failed_bronze_files(self, endpoint: str):
        return []

    async def insert_job(self, source_system, source_url, endpoint, file_type, bytes_size, content_hash, batch_id, file_path):
        # dedupe by content_hash
        if content_hash in (j.get('content_hash') for j in self.jobs.values()):
            return None
        ingestion_id = f"mock-{len(self.jobs)+1}"
        self.jobs[ingestion_id] = {
            'ingestion_id': ingestion_id,
            'batch_id': batch_id,
            'file_path': file_path,
            'content_hash': content_hash,
            'file_type': file_type
        }
        return ingestion_id

    async def start_bronze_job(self, ingestion_id: str) -> bool:
        return True

    async def complete_bronze_job(self, ingestion_id: str) -> bool:
        return True

    async def fail_bronze(self, ingestion_id: str, error_msg: str) -> bool:
        job = self.jobs.get(ingestion_id)
        if job:
            job['error'] = error_msg
            return True
        return False

    async def fetch_pending_silver_files(self):
        # create silver entries from existing jobs
        files = []
        for job in self.jobs.values():
            files.append({
                'ingestion_id': job['ingestion_id'],
                'batch_id': job['batch_id'],
                'file_path': job['file_path']
            })
        return files

    async def start_silver(self, ingestion_id: str) -> bool:
        return True

    async def complete_silver(self, ingestion_id: str) -> bool:
        return True

    async def fail_silver(self, ingestion_id: str, error_msg: str) -> bool:
        job = self.jobs.get(ingestion_id)
        if job:
            job['error_silver'] = error_msg
            return True
        return False

async def main():
    tracker = MockTracker()
    ticketmaster = MockTicketMasterClient()
    supabase = MockSupaBaseClient()

    # Run bronze (this will upload to mock supabase and record job in tracker)
    await bronze_main.run_test(
        tracker=tracker,
        ticketmaster=ticketmaster,
        supabase=supabase
    )

    # Now simulate prep_silver_files behavior: fetch pending files and download content
    files = await tracker.fetch_pending_silver_files()

    for f in files:
        content = supabase.download_file(bucket_name='mock', file_path=f['file_path'])
        if not content:
            print('No content found for', f['file_path'])
            continue
        # load into polars via utility
        from src.data_workflows.pipelines.common.file_loader import load_file
        df = load_file(file_bytes=content, file_type=FileExtensions.JSON)

        transformed = silver_transform.silver_events_transformations(df)
        print('Transformed rows:', len(transformed))
        print(transformed)

if __name__ == '__main__':
    asyncio.run(main())
