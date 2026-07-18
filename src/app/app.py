from fastapi import FastAPI
from src.app.routes import router
from src.services.utils.logs import setup_logging


setup_logging()

app = FastAPI(
    title="Ingestion Tracker API",
    version="1.0.0"
)

app.include_router(router)


@app.get("/")
def health_check():
    return {"status": "running"}