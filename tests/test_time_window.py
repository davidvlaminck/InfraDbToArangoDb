import datetime as dt

from utils.time_window import (
	BRUSSELS,
	get_time_window_label,
	is_within_time_window,
	seconds_until_next_window_start,
)


def _dt(value: str) -> dt.datetime:
	return dt.datetime.fromisoformat(value).replace(tzinfo=BRUSSELS)


def test_is_within_time_window_for_standard_window():
	time_conf = {"start": "06:00:00", "end": "23:50:00"}

	assert is_within_time_window(time_conf, now=_dt("2026-04-24T06:00:00")) is True
	assert is_within_time_window(time_conf, now=_dt("2026-04-24T23:50:00")) is True
	assert is_within_time_window(time_conf, now=_dt("2026-04-24T05:59:59")) is False


def test_is_within_time_window_for_window_that_spans_midnight():
	time_conf = {"start": "22:00:00", "end": "03:00:00"}

	assert is_within_time_window(time_conf, now=_dt("2026-04-24T23:30:00")) is True
	assert is_within_time_window(time_conf, now=_dt("2026-04-25T02:30:00")) is True
	assert is_within_time_window(time_conf, now=_dt("2026-04-24T12:00:00")) is False


def test_missing_time_window_allows_run():
	assert is_within_time_window(None, now=_dt("2026-04-24T12:00:00")) is True
	assert get_time_window_label(None) == "always"
	assert seconds_until_next_window_start(None, now=_dt("2026-04-24T12:00:00")) == 0.0


def test_seconds_until_next_window_start_uses_next_day_when_needed():
	time_conf = {"start": "06:00:00", "end": "23:50:00"}

	delta = seconds_until_next_window_start(time_conf, now=_dt("2026-04-24T23:55:00"))

	assert delta == 6 * 60 * 60 + 5 * 60

