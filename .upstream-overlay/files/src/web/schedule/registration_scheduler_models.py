from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ScheduledRegistrationRequest(BaseModel):
    """定时注册请求"""
    schedule_cron: str = "*/30 * * * *"
    schedule_interval_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    run_immediately: bool = True
    registration_mode: str = "batch"
    email_service_type: str = "tempmail"
    proxy: Optional[str] = None
    email_service_config: Optional[dict] = None
    email_service_id: Optional[int] = None
    count: int = 100
    interval_min: int = 5
    interval_max: int = 30
    concurrency: int = 1
    mode: str = "pipeline"
    auto_upload_cpa: bool = False
    cpa_service_ids: List[int] = []
    auto_upload_sub2api: bool = False
    sub2api_service_ids: List[int] = []
    auto_upload_tm: bool = False
    tm_service_ids: List[int] = []


class ScheduledRegistrationStatusResponse(BaseModel):
    """定时注册状态响应"""
    enabled: bool
    schedule_cron: str
    timezone: str
    registration_mode: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    next_run_at: Optional[str] = None
    last_run_at: Optional[str] = None
    last_error: Optional[str] = None
    total_runs: int = 0
    success_runs: int = 0
    failed_runs: int = 0
    skipped_runs: int = 0
    active_task_uuid: Optional[str] = None
    active_batch_id: Optional[str] = None
