from fastapi import APIRouter, HTTPException

from .registration_scheduler import (
    get_scheduled_registration_status,
    start_scheduled_registration,
    stop_scheduled_registration,
)
from .registration_scheduler_models import (
    ScheduledRegistrationRequest,
    ScheduledRegistrationStatusResponse,
)

router = APIRouter()


@router.get("/schedule/status", response_model=ScheduledRegistrationStatusResponse)
async def schedule_status():
    return get_scheduled_registration_status()


@router.post("/schedule/start", response_model=ScheduledRegistrationStatusResponse)
async def schedule_start(request: ScheduledRegistrationRequest):
    try:
        return await start_scheduled_registration(request)
    except HTTPException:
        raise


@router.post("/schedule/stop", response_model=ScheduledRegistrationStatusResponse)
async def schedule_stop():
    try:
        return await stop_scheduled_registration()
    except HTTPException:
        raise
