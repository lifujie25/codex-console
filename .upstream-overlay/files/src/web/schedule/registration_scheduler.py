import asyncio
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException

from ...config.settings import get_settings, update_settings
from ...database import crud
from ...database.session import get_db
from ..routes.registration import (
    BatchRegistrationRequest,
    RegistrationTaskCreate,
    _enqueue_batch_registration,
    _enqueue_single_registration,
    _validate_batch_params,
    _validate_email_service_type,
    batch_tasks,
)
from .registration_scheduler_models import (
    ScheduledRegistrationRequest,
    ScheduledRegistrationStatusResponse,
)

logger = logging.getLogger(__name__)

scheduled_registration_lock = threading.Lock()
scheduled_registration: Dict[str, Any] = {
    "enabled": False,
    "schedule_cron": "*/30 * * * *",
    "timezone": "Asia/Shanghai",
    "registration_mode": "batch",
    "payload": None,
    "next_run_at": None,
    "last_run_at": None,
    "last_error": None,
    "total_runs": 0,
    "success_runs": 0,
    "failed_runs": 0,
    "skipped_runs": 0,
    "active_task_uuid": None,
    "active_batch_id": None,
}
scheduled_registration_runner: Optional[asyncio.Task] = None


def _get_schedule_timezone_name() -> str:
    tz_name = (os.environ.get("TZ") or "Asia/Shanghai").strip()
    return tz_name or "Asia/Shanghai"


def _get_schedule_timezone():
    tz_name = _get_schedule_timezone_name()
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        logger.warning(f"无效 TZ={tz_name}，将回退到 Asia/Shanghai")
        try:
            return ZoneInfo("Asia/Shanghai")
        except ZoneInfoNotFoundError:
            logger.warning("系统缺少 Asia/Shanghai 时区数据，回退到固定 +08:00")
            return timezone(timedelta(hours=8), name="Asia/Shanghai")


def _now_in_schedule_tz() -> datetime:
    return datetime.now(_get_schedule_timezone())


def _dt_to_iso(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=_get_schedule_timezone())
    return value.isoformat()


def _legacy_interval_to_cron(interval_minutes: int) -> str:
    if interval_minutes < 1 or interval_minutes > 1440:
        raise ValueError("旧版 interval 必须在 1-1440 分钟之间")

    if interval_minutes <= 59:
        return f"*/{interval_minutes} * * * *"
    if interval_minutes == 60:
        return "0 * * * *"
    if interval_minutes % 60 == 0:
        hours = interval_minutes // 60
        if 1 <= hours <= 23:
            return f"0 */{hours} * * *"
        if hours == 24:
            return "0 0 * * *"

    raise ValueError("旧版 interval 仅支持 1-59、整小时或 24 小时，请改用 5 位 Cron 表达式")


def _cron_to_legacy_interval_minutes(cron_expr: str) -> int:
    parts = " ".join((cron_expr or "").split()).split(" ")
    if len(parts) != 5:
        return 30

    minute, hour, day, month, weekday = parts
    if hour == "*" and day == "*" and month == "*" and weekday == "*" and minute.startswith("*/"):
        try:
            val = int(minute[2:])
            if 1 <= val <= 59:
                return val
        except ValueError:
            return 30

    if minute == "0" and hour == "*" and day == "*" and month == "*" and weekday == "*":
        return 60

    if minute == "0" and day == "*" and month == "*" and weekday == "*" and hour.startswith("*/"):
        try:
            val = int(hour[2:])
            if 1 <= val <= 23:
                return val * 60
        except ValueError:
            return 30

    if minute == "0" and hour == "0" and day == "*" and month == "*" and weekday == "*":
        return 1440

    return 30


def _parse_cron_field(field: str, min_value: int, max_value: int, field_name: str, is_weekday: bool = False) -> set:
    values = set()
    tokens = [token.strip() for token in field.split(",") if token.strip()]
    if not tokens:
        raise ValueError(f"{field_name} 字段不能为空")

    for token in tokens:
        step = 1
        base = token
        if "/" in token:
            base, step_str = token.split("/", 1)
            if not step_str.isdigit():
                raise ValueError(f"{field_name} 字段步长无效: {token}")
            step = int(step_str)
            if step <= 0:
                raise ValueError(f"{field_name} 字段步长必须大于 0: {token}")

        def _normalize(v: int) -> int:
            if is_weekday and v == 7:
                return 0
            return v

        def _validate(v: int) -> int:
            nv = _normalize(v)
            upper = 6 if is_weekday else max_value
            lower = min_value
            if nv < lower or nv > upper:
                raise ValueError(f"{field_name} 字段超出范围 {min_value}-{max_value}: {token}")
            return nv

        if base in ("*", ""):
            for raw in range(min_value, max_value + 1, step):
                values.add(_normalize(raw))
            continue

        if "-" in base:
            start_str, end_str = base.split("-", 1)
            if not start_str.isdigit() or not end_str.isdigit():
                raise ValueError(f"{field_name} 字段范围无效: {token}")
            start = int(start_str)
            end = int(end_str)
            if start > end:
                raise ValueError(f"{field_name} 字段范围无效: {token}")
            for raw in range(start, end + 1, step):
                values.add(_validate(raw))
            continue

        if not base.isdigit():
            raise ValueError(f"{field_name} 字段值无效: {token}")

        start = int(base)
        if step == 1:
            values.add(_validate(start))
        else:
            for raw in range(start, max_value + 1, step):
                values.add(_validate(raw))

    return values


def _parse_cron_expression(cron_expr: str):
    normalized = " ".join((cron_expr or "").split())
    parts = normalized.split(" ")
    if len(parts) != 5:
        raise ValueError("Cron 必须是 5 位（分 时 日 月 周）")

    minute_field, hour_field, day_field, month_field, weekday_field = parts
    parsed = {
        "minute": _parse_cron_field(minute_field, 0, 59, "分钟"),
        "hour": _parse_cron_field(hour_field, 0, 23, "小时"),
        "day": _parse_cron_field(day_field, 1, 31, "日期"),
        "month": _parse_cron_field(month_field, 1, 12, "月份"),
        "weekday": _parse_cron_field(weekday_field, 0, 7, "星期", is_weekday=True),
        "day_any": day_field == "*",
        "weekday_any": weekday_field == "*",
    }
    return normalized, parsed


def _validate_schedule_cron_or_raise(cron_expr: str) -> str:
    try:
        normalized, _ = _parse_cron_expression(cron_expr)
        return normalized
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Cron 表达式无效: {e}")


def _cron_day_matches(dt: datetime, parsed: Dict[str, Any]) -> bool:
    day_match = dt.day in parsed["day"]
    weekday_now = (dt.weekday() + 1) % 7
    weekday_match = weekday_now in parsed["weekday"]

    if parsed["day_any"] and parsed["weekday_any"]:
        return True
    if parsed["day_any"]:
        return weekday_match
    if parsed["weekday_any"]:
        return day_match
    return day_match or weekday_match


def _cron_matches(dt: datetime, parsed: Dict[str, Any]) -> bool:
    return (
        dt.minute in parsed["minute"]
        and dt.hour in parsed["hour"]
        and dt.month in parsed["month"]
        and _cron_day_matches(dt, parsed)
    )


def _get_next_run_from_cron(cron_expr: str, after_dt: Optional[datetime] = None) -> datetime:
    normalized, parsed = _parse_cron_expression(cron_expr)
    tz = _get_schedule_timezone()
    now = (after_dt or _now_in_schedule_tz()).astimezone(tz)
    probe = now.replace(second=0, microsecond=0) + timedelta(minutes=1)

    for _ in range(2 * 366 * 24 * 60):
        if _cron_matches(probe, parsed):
            return probe
        probe += timedelta(minutes=1)

    raise RuntimeError(f"无法计算下一次触发时间，请检查 Cron: {normalized}")


def get_scheduled_registration_status() -> ScheduledRegistrationStatusResponse:
    with scheduled_registration_lock:
        state = scheduled_registration.copy()

    return ScheduledRegistrationStatusResponse(
        enabled=bool(state.get("enabled")),
        schedule_cron=state.get("schedule_cron") or "*/30 * * * *",
        timezone=state.get("timezone") or _get_schedule_timezone_name(),
        registration_mode=state.get("registration_mode") or "batch",
        payload=state.get("payload") or {},
        next_run_at=_dt_to_iso(state.get("next_run_at")),
        last_run_at=_dt_to_iso(state.get("last_run_at")),
        last_error=state.get("last_error"),
        total_runs=int(state.get("total_runs") or 0),
        success_runs=int(state.get("success_runs") or 0),
        failed_runs=int(state.get("failed_runs") or 0),
        skipped_runs=int(state.get("skipped_runs") or 0),
        active_task_uuid=state.get("active_task_uuid"),
        active_batch_id=state.get("active_batch_id"),
    )


def _save_scheduled_registration_settings(enabled: bool, schedule_cron: str, payload: Optional[dict] = None):
    cron_normalized = _validate_schedule_cron_or_raise(schedule_cron)
    update_settings(
        registration_schedule_enabled=enabled,
        registration_schedule_interval_minutes=_cron_to_legacy_interval_minutes(cron_normalized),
        registration_schedule_cron=cron_normalized,
        registration_schedule_payload=payload or {},
    )


async def restore_scheduled_registration_from_settings():
    global scheduled_registration_runner
    settings = get_settings()
    tz_name = _get_schedule_timezone_name()

    enabled = bool(settings.registration_schedule_enabled)
    saved_cron = (getattr(settings, "registration_schedule_cron", "") or "").strip()
    legacy_interval = int(settings.registration_schedule_interval_minutes or 30)
    raw_payload = settings.registration_schedule_payload or {}

    fallback_cron = saved_cron or _legacy_interval_to_cron(legacy_interval)
    payload: dict = {}
    schedule_cron = fallback_cron

    if raw_payload:
        try:
            normalized_payload = dict(raw_payload)
            if not normalized_payload.get("schedule_cron"):
                legacy = normalized_payload.get("schedule_interval_minutes")
                if legacy is not None:
                    normalized_payload["schedule_cron"] = _legacy_interval_to_cron(int(legacy))
                else:
                    normalized_payload["schedule_cron"] = fallback_cron
            request_payload = ScheduledRegistrationRequest(**normalized_payload)
            payload = request_payload.model_dump(exclude_none=True)
            schedule_cron = payload.get("schedule_cron", fallback_cron)
        except Exception as e:
            logger.warning(f"定时注册配置恢复失败，将自动禁用: {e}")
            enabled = False
            payload = {}
            try:
                _save_scheduled_registration_settings(False, fallback_cron, {})
            except Exception:
                pass
    elif enabled:
        logger.warning("定时注册已启用但缺少有效配置，已自动禁用")
        enabled = False
        try:
            _save_scheduled_registration_settings(False, fallback_cron, {})
        except Exception:
            pass

    try:
        schedule_cron = _validate_schedule_cron_or_raise(schedule_cron)
    except HTTPException as e:
        logger.warning(f"定时注册 Cron 无效，将自动禁用: {e.detail}")
        enabled = False
        payload = {}
        schedule_cron = "*/30 * * * *"
        try:
            _save_scheduled_registration_settings(False, schedule_cron, {})
        except Exception:
            pass

    now = _now_in_schedule_tz()
    with scheduled_registration_lock:
        scheduled_registration["enabled"] = enabled and bool(payload)
        scheduled_registration["schedule_cron"] = schedule_cron
        scheduled_registration["timezone"] = tz_name
        scheduled_registration["registration_mode"] = payload.get("registration_mode", "batch") if payload else "batch"
        scheduled_registration["payload"] = payload
        scheduled_registration["next_run_at"] = _get_next_run_from_cron(schedule_cron, now) if (enabled and payload) else None
        scheduled_registration["last_run_at"] = None
        scheduled_registration["last_error"] = None
        scheduled_registration["total_runs"] = 0
        scheduled_registration["success_runs"] = 0
        scheduled_registration["failed_runs"] = 0
        scheduled_registration["skipped_runs"] = 0
        scheduled_registration["active_task_uuid"] = None
        scheduled_registration["active_batch_id"] = None

    if scheduled_registration["enabled"]:
        if not scheduled_registration_runner or scheduled_registration_runner.done():
            scheduled_registration_runner = asyncio.create_task(_scheduled_registration_loop())
        logger.info(
            "已从 settings 恢复定时注册：cron=%s, tz=%s, mode=%s",
            scheduled_registration["schedule_cron"],
            scheduled_registration["timezone"],
            scheduled_registration["registration_mode"],
        )


def _has_active_scheduled_job() -> bool:
    with scheduled_registration_lock:
        active_task_uuid = scheduled_registration.get("active_task_uuid")
        active_batch_id = scheduled_registration.get("active_batch_id")

    if active_task_uuid:
        with get_db() as db:
            task = crud.get_registration_task(db, active_task_uuid)
            if task and task.status in ("pending", "running"):
                return True

    if active_batch_id:
        batch = batch_tasks.get(active_batch_id)
        if batch and not batch.get("finished", False):
            return True

    return False


async def _run_scheduled_registration_once():
    with scheduled_registration_lock:
        payload = dict(scheduled_registration.get("payload") or {})
        registration_mode = scheduled_registration.get("registration_mode") or "batch"

    if not payload:
        raise RuntimeError("定时任务配置为空")

    common_kwargs = {
        "email_service_type": payload.get("email_service_type") or "tempmail",
        "proxy": payload.get("proxy"),
        "email_service_config": payload.get("email_service_config"),
        "email_service_id": payload.get("email_service_id"),
        "auto_upload_cpa": bool(payload.get("auto_upload_cpa")),
        "cpa_service_ids": payload.get("cpa_service_ids") or [],
        "auto_upload_sub2api": bool(payload.get("auto_upload_sub2api")),
        "sub2api_service_ids": payload.get("sub2api_service_ids") or [],
        "auto_upload_tm": bool(payload.get("auto_upload_tm")),
        "tm_service_ids": payload.get("tm_service_ids") or [],
    }

    if registration_mode == "single":
        response = await _enqueue_single_registration(RegistrationTaskCreate(**common_kwargs))
        with scheduled_registration_lock:
            scheduled_registration["active_task_uuid"] = response.task_uuid
            scheduled_registration["active_batch_id"] = None
        logger.info(f"定时注册触发成功（单次任务）: {response.task_uuid}")
        return

    if registration_mode == "batch":
        response = await _enqueue_batch_registration(BatchRegistrationRequest(
            **common_kwargs,
            count=int(payload.get("count") or 100),
            interval_min=int(payload.get("interval_min") or 5),
            interval_max=int(payload.get("interval_max") or 30),
            concurrency=int(payload.get("concurrency") or 1),
            mode=payload.get("mode") or "pipeline",
        ))
        with scheduled_registration_lock:
            scheduled_registration["active_task_uuid"] = None
            scheduled_registration["active_batch_id"] = response.batch_id
        logger.info(f"定时注册触发成功（批量任务）: {response.batch_id}")
        return

    raise RuntimeError(f"不支持的定时注册模式: {registration_mode}")


async def _scheduled_registration_loop():
    global scheduled_registration_runner
    logger.info("定时注册调度器已启动 (tz=%s)", _get_schedule_timezone_name())

    try:
        while True:
            await asyncio.sleep(1)

            with scheduled_registration_lock:
                enabled = bool(scheduled_registration.get("enabled"))
                schedule_cron = scheduled_registration.get("schedule_cron") or "*/30 * * * *"
                next_run_at = scheduled_registration.get("next_run_at")

            if not enabled:
                break

            now = _now_in_schedule_tz()
            if not next_run_at:
                with scheduled_registration_lock:
                    scheduled_registration["next_run_at"] = _get_next_run_from_cron(schedule_cron, now)
                continue

            if now < next_run_at:
                continue

            if _has_active_scheduled_job():
                with scheduled_registration_lock:
                    scheduled_registration["next_run_at"] = _get_next_run_from_cron(schedule_cron, now)
                    scheduled_registration["skipped_runs"] = int(scheduled_registration.get("skipped_runs") or 0) + 1
                logger.info("定时注册本轮跳过：上一轮任务仍在执行")
                continue

            with scheduled_registration_lock:
                scheduled_registration["last_run_at"] = now
                scheduled_registration["next_run_at"] = _get_next_run_from_cron(schedule_cron, now)
                scheduled_registration["last_error"] = None

            try:
                await _run_scheduled_registration_once()
                with scheduled_registration_lock:
                    scheduled_registration["total_runs"] = int(scheduled_registration.get("total_runs") or 0) + 1
                    scheduled_registration["success_runs"] = int(scheduled_registration.get("success_runs") or 0) + 1
            except Exception as e:
                err_message = e.detail if isinstance(e, HTTPException) else str(e)
                logger.error(f"定时注册触发失败: {err_message}")
                with scheduled_registration_lock:
                    scheduled_registration["total_runs"] = int(scheduled_registration.get("total_runs") or 0) + 1
                    scheduled_registration["failed_runs"] = int(scheduled_registration.get("failed_runs") or 0) + 1
                    scheduled_registration["last_error"] = err_message
    except asyncio.CancelledError:
        logger.info("定时注册调度器已停止")
        raise
    finally:
        scheduled_registration_runner = None


async def start_scheduled_registration(request: ScheduledRegistrationRequest) -> ScheduledRegistrationStatusResponse:
    global scheduled_registration_runner

    if request.registration_mode not in ("single", "batch"):
        raise HTTPException(status_code=400, detail="定时注册模式必须为 single 或 batch")

    _validate_email_service_type(request.email_service_type)
    if request.registration_mode == "batch":
        _validate_batch_params(
            count=request.count,
            interval_min=request.interval_min,
            interval_max=request.interval_max,
            concurrency=request.concurrency,
            mode=request.mode,
        )

    try:
        if request.schedule_interval_minutes is not None:
            schedule_cron = _legacy_interval_to_cron(int(request.schedule_interval_minutes))
        else:
            schedule_cron = request.schedule_cron
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    schedule_cron = _validate_schedule_cron_or_raise(schedule_cron)
    payload_to_store = request.model_dump(exclude_none=True)
    payload_to_store["schedule_cron"] = schedule_cron
    payload_to_store.pop("schedule_interval_minutes", None)

    now = _now_in_schedule_tz()
    first_run_at = now if request.run_immediately else _get_next_run_from_cron(schedule_cron, now)
    tz_name = _get_schedule_timezone_name()

    with scheduled_registration_lock:
        scheduled_registration["enabled"] = True
        scheduled_registration["schedule_cron"] = schedule_cron
        scheduled_registration["timezone"] = tz_name
        scheduled_registration["registration_mode"] = request.registration_mode
        scheduled_registration["payload"] = payload_to_store
        scheduled_registration["next_run_at"] = first_run_at
        scheduled_registration["last_run_at"] = None
        scheduled_registration["last_error"] = None
        scheduled_registration["total_runs"] = 0
        scheduled_registration["success_runs"] = 0
        scheduled_registration["failed_runs"] = 0
        scheduled_registration["skipped_runs"] = 0
        scheduled_registration["active_task_uuid"] = None
        scheduled_registration["active_batch_id"] = None

    try:
        _save_scheduled_registration_settings(True, schedule_cron, payload_to_store)
    except Exception as e:
        logger.error(f"保存定时注册设置失败: {e}")
        raise HTTPException(status_code=500, detail=f"保存定时注册设置失败: {e}")

    old_runner = scheduled_registration_runner
    if old_runner and not old_runner.done():
        old_runner.cancel()
        try:
            await old_runner
        except asyncio.CancelledError:
            pass

    scheduled_registration_runner = asyncio.create_task(_scheduled_registration_loop())

    logger.info(
        "定时注册已启动：cron=%s, tz=%s, mode=%s, first_run_at=%s",
        schedule_cron,
        tz_name,
        request.registration_mode,
        first_run_at.isoformat(),
    )
    return get_scheduled_registration_status()


async def stop_scheduled_registration() -> ScheduledRegistrationStatusResponse:
    global scheduled_registration_runner

    with scheduled_registration_lock:
        schedule_cron = scheduled_registration.get("schedule_cron") or "*/30 * * * *"
        payload = dict(scheduled_registration.get("payload") or {})
        scheduled_registration["enabled"] = False
        scheduled_registration["next_run_at"] = None

    try:
        _save_scheduled_registration_settings(False, schedule_cron, payload)
    except Exception as e:
        logger.error(f"保存定时注册设置失败: {e}")
        raise HTTPException(status_code=500, detail=f"保存定时注册设置失败: {e}")

    runner = scheduled_registration_runner
    if runner and not runner.done():
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass

    logger.info("定时注册已停止")
    return get_scheduled_registration_status()
