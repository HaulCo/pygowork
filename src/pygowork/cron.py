"""Parses gocraft/work periodic job specs: robfig/cron v1 format, which is a
six-field cron expression with seconds first, plus the @descriptors and
@every <duration>. Backed by croniter (second_at_beginning matches robfig's
field order); @every durations are handled directly."""

import re
from datetime import datetime, timedelta

from croniter import croniter

DESCRIPTORS = {
    "@yearly": "0 0 0 1 1 *",
    "@annually": "0 0 0 1 1 *",
    "@monthly": "0 0 0 1 * *",
    "@weekly": "0 0 0 * * 0",
    "@daily": "0 0 0 * * *",
    "@midnight": "0 0 0 * * *",
    "@hourly": "0 0 * * * *",
}

GO_DURATION_UNITS = {"h": 3600, "m": 60, "s": 1, "ms": 0.001}
GO_DURATION_PATTERN = re.compile(r"(\d+(?:\.\d+)?)(h|ms|m|s)")


def parse_go_duration(text: str) -> timedelta:
    total = 0.0
    matched = GO_DURATION_PATTERN.findall(text.strip())
    if not matched or "".join(f"{value}{unit}" for value, unit in matched) != text.strip():
        raise ValueError(f"unparseable duration: {text!r}")
    for value, unit in matched:
        total += float(value) * GO_DURATION_UNITS[unit]
    return timedelta(seconds=total)


class Schedule:
    """next(after) semantics matching robfig's cron.Schedule."""

    def __init__(self, spec: str) -> None:
        self.spec = spec
        self._interval: timedelta | None = None
        self._expression: str | None = None
        if spec.startswith("@every "):
            self._interval = parse_go_duration(spec.removeprefix("@every "))
        else:
            self._expression = DESCRIPTORS.get(spec, spec)
            croniter(self._expression, second_at_beginning=True)  # validate eagerly

    def next_after(self, moment: datetime) -> datetime:
        if self._interval is not None:
            return moment + self._interval
        return croniter(self._expression, moment, second_at_beginning=True).get_next(datetime)
