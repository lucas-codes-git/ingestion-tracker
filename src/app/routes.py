from fastapi import APIRouter, status, Request
from src.data_workflows.pipelines.sources.ticketmaster.events.bronze.main import run_test

router = APIRouter(
    prefix="/api/v1",
    tags=["ticketmaster"]
)


@router.post("/ticketmaster/attractions")
async def get_ticketmaster_attractions():
    return {
        "message": "ticketmaster attractions endpoint"
    }


@router.post("/ticketmaster/events", status_code=status.HTTP_202_ACCEPTED)
async def get_ticketmaster_events(request: Request):
    await run_test(request.app.state.tracker)
    return {
        "message": "ticketmaster/events route completed",
    }


@router.post("/ticketmaster/genres")
async def get_ticketmaster_genres():
    return {
        "message": "ticketmaster genres endpoint"
    }