from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.app.routes import router
from src.services.utils.logs import setup_logging
from src.services.tracker import IngestionTracker


setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):

    tracker = IngestionTracker()

    tracker.start()
    tracker.create_ingestion_tracker_table()

    app.state.tracker = tracker

    yield

    tracker.close()


app = FastAPI(
    title="Ingestion Tracker API",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(router)


@app.get("/")
def health_check():
    return {"status": "running"}