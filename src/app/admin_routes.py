from fastapi import APIRouter, Request, HTTPException
from typing import Optional

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/failed/silver")
async def list_failed_silver(endpoint: str, request: Request):
    tracker = request.app.state.tracker
    files = await tracker.fetch_failed_silver_files(endpoint)
    return {"failed": files}


@router.post("/retry/silver")
async def retry_silver(batch_id: Optional[str] = None, endpoint: Optional[str] = None, request: Request = None):
    tracker = request.app.state.tracker
    supa = request.app.state.supabase

    if batch_id:
        job = await tracker.retry_failed_silver_by_batch(batch_id)
        if not job:
            raise HTTPException(status_code=404, detail="No failed silver job found for batch_id")
        # trigger silver pipeline for the endpoint (reprocess pending silver files)
        from src.data_workflows.pipelines.sources.ticketmaster.events.silver.main import run_silver_ticketmaster_events
        await run_silver_ticketmaster_events(tracker=tracker, supa=supa)
        return {"requeued": job}

    if endpoint:
        files = await tracker.retry_failed_silver_files(endpoint)
        if not files:
            return {"requeued": []}
        from src.data_workflows.pipelines.sources.ticketmaster.events.silver.main import run_silver_ticketmaster_events
        await run_silver_ticketmaster_events(tracker=tracker, supa=supa)
        return {"requeued": files}

    raise HTTPException(status_code=400, detail="Provide batch_id or endpoint")
