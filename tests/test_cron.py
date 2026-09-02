"""Coverage for cron.py: robfig six-field specs, the @descriptors, and
@every Go durations. Upstream leans on robfig/cron's own test suite for
this; the parsing here is pygowork's own code, so it gets its own tests."""

from datetime import datetime, timedelta

import pytest

from pygowork import Schedule
from pygowork.cron import parse_go_duration


def test_six_field_spec_with_seconds():
    schedule = Schedule("30 0 * * * *")
    assert schedule.next_after(datetime(2026, 1, 1, 12, 0, 0)) == datetime(2026, 1, 1, 12, 0, 30)


def test_next_after_is_strictly_after():
    schedule = Schedule("0 * * * * *")
    exactly_on_tick = datetime(2026, 1, 1, 12, 5, 0)
    assert schedule.next_after(exactly_on_tick) == datetime(2026, 1, 1, 12, 6, 0)


def test_hourly_descriptor():
    schedule = Schedule("@hourly")
    assert schedule.next_after(datetime(2026, 1, 1, 0, 30, 15)) == datetime(2026, 1, 1, 1, 0, 0)


def test_daily_and_midnight_descriptors_agree():
    moment = datetime(2026, 1, 1, 13, 30, 0)
    assert Schedule("@daily").next_after(moment) == Schedule("@midnight").next_after(moment)
    assert Schedule("@daily").next_after(moment) == datetime(2026, 1, 2, 0, 0, 0)


def test_every_interval():
    schedule = Schedule("@every 90s")
    moment = datetime(2026, 1, 1, 12, 0, 0)
    assert schedule.next_after(moment) == moment + timedelta(seconds=90)


def test_parse_go_duration():
    assert parse_go_duration("1h30m") == timedelta(seconds=5400)
    assert parse_go_duration("90s") == timedelta(seconds=90)
    assert parse_go_duration("500ms") == timedelta(milliseconds=500)
    assert parse_go_duration("1.5h") == timedelta(seconds=5400)


def test_parse_go_duration_rejects_garbage():
    with pytest.raises(ValueError):
        parse_go_duration("wat")
    with pytest.raises(ValueError):
        parse_go_duration("10x")


def test_bad_cron_spec_rejected_at_construction():
    with pytest.raises(ValueError):
        Schedule("not a cron spec")
