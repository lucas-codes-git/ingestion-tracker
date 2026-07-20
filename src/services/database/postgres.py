from psycopg_pool import AsyncConnectionPool
from src.services.utils import fetch_secrets

env = fetch_secrets()

pool = AsyncConnectionPool(
    conninfo=env["supadburl"],
    min_size=1,
    max_size=5,
    open=False
)