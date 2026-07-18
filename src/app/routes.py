from fastapi import APIRouter
from src.data_workflows.pipelines.sources.ticketmaster.events.bronze.main import run_test
import asyncio

router = APIRouter(
    prefix="/api/v1",
    tags=["ticketmaster"]
)


@router.post("/ticketmaster/attractions")
async def get_ticketmaster_attractions():
    return {
        "message": "ticketmaster attractions endpoint"
    }


@router.post("/ticketmaster/events")
async def get_ticketmaster_events():
    result = await run_test()
    return result, 200


@router.post("/ticketmaster/genres")
async def get_ticketmaster_genres():
    return {
        "message": "ticketmaster genres endpoint"
    }