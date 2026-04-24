from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

BRUSSELS = ZoneInfo("Europe/Brussels")
DEFAULT_TIME_WINDOW_LABEL = "always"


def parse_hms_to_time(value: str | None) -> dt.time:
    if not value:
        raise ValueError("Expected HH:MM:SS value")
    return dt.datetime.strptime(value, "%H:%M:%S").time()


def get_time_window_bounds(time_conf: dict | None) -> tuple[dt.time, dt.time] | None:
    if not isinstance(time_conf, dict):
        return None

    start_value = time_conf.get("start")
    end_value = time_conf.get("end")
    if not start_value or not end_value:
        return None

    return parse_hms_to_time(start_value), parse_hms_to_time(end_value)


def is_within_time_window(
    time_conf: dict | None,
    *,
    now: dt.datetime | None = None,
    timezone: ZoneInfo = BRUSSELS,
) -> bool:
    bounds = get_time_window_bounds(time_conf)
    if bounds is None:
        return True

    start_time, end_time = bounds
    current_dt = now.astimezone(timezone) if now is not None else dt.datetime.now(timezone)
    current_time = current_dt.time().replace(microsecond=0)

    if start_time <= end_time:
        return start_time <= current_time <= end_time
    return current_time >= start_time or current_time <= end_time


def seconds_until_next_window_start(
    time_conf: dict | None,
    *,
    now: dt.datetime | None = None,
    timezone: ZoneInfo = BRUSSELS,
) -> float:
    bounds = get_time_window_bounds(time_conf)
    if bounds is None:
        return 0.0

    start_time, _ = bounds
    current_dt = now.astimezone(timezone) if now is not None else dt.datetime.now(timezone)
    next_start = current_dt.replace(
        hour=start_time.hour,
        minute=start_time.minute,
        second=start_time.second,
        microsecond=0,
    )
    if next_start <= current_dt:
        next_start += dt.timedelta(days=1)

    return max(0.0, (next_start - current_dt).total_seconds())


def get_time_window_label(time_conf: dict | None) -> str:
    bounds = get_time_window_bounds(time_conf)
    if bounds is None:
        return DEFAULT_TIME_WINDOW_LABEL

    start_time, end_time = bounds
    return f"{start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}"

