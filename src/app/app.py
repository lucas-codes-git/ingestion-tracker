from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.app.routes import router
from src.services.database import pool
from src.services.utils.logs import setup_logging
from src.services.tracker import IngestionTracker
from src.services.clients import TicketMasterClient, SupaBaseClient


setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    
    await pool.open()
    await pool.wait()
    
    ticketmaster = TicketMasterClient()
    supabase = SupaBaseClient()

    tracker = IngestionTracker()

    await tracker.create_ingestion_tracker_table()
    
    app.state.ticketmaster = ticketmaster
    app.state.supabase = supabase
    app.state.tracker = tracker

    yield
    
    await ticketmaster.close()
    await pool.close()


app = FastAPI(
    title="Ingestion Tracker API",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(router)


@app.get("/")
def health_check():
    return {"status": "running"}