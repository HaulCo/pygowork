"""The Job type, mirroring gocraft/work's Job struct and its wire format:
compact JSON with keys "name", "id", "t", "args", plus "unique" when set and
"fails"/"err"/"failed_at" once a job has failed (omitted otherwise, matching
Go's omitempty)."""

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


class ArgumentError(Exception):
    """Raised by Job.arg for a missing or mistyped job argument."""


@dataclass
class Job:
    name: str
    id: str
    enqueued_at: int
    args: dict[str, Any] | None
    unique: bool = False
    fails: int = 0
    last_err: str | None = None
    failed_at: int | None = None

    # Set on fetched jobs, mirroring Go's rawJSON / dequeuedFrom / inProgQueue.
    raw: bytes = b""
    dequeued_from: str = ""
    in_progress_queue: str = ""
    checkin: Callable[[str], None] = field(default=lambda message: None)

    @classmethod
    def from_wire(
        cls, raw: bytes, dequeued_from: str = "", in_progress_queue: str = ""
    ) -> "Job":
        payload = json.loads(raw)
        return cls(
            name=payload["name"],
            id=payload.get("id", ""),
            enqueued_at=payload.get("t", 0),
            args=payload.get("args"),
            unique=bool(payload.get("unique")),
            fails=int(payload.get("fails", 0)),
            last_err=payload.get("err"),
            failed_at=payload.get("failed_at"),
            raw=raw,
            dequeued_from=dequeued_from,
            in_progress_queue=in_progress_queue,
        )

    def arg(self, key: str, expected_type: type) -> Any:
        """Typed access to one argument: raises ArgumentError naming the job,
        the key, and both types when the key is missing or the value has the
        wrong type. Raising inside a handler fails the job, so the message
        lands in last_err where the retry and dead sets surface it.

        Two JSON realities are accounted for: a whole-number float from a Go
        producer arrives as an int (Go marshals float64(1.0) as 1), so
        expected_type float accepts ints and returns them as float; and bool
        subclasses int in Python, so True never passes for an int."""
        if self.args is None or key not in self.args:
            raise ArgumentError(f"job {self.name!r} arg {key!r}: missing")
        value = self.args[key]
        if isinstance(value, bool) and expected_type is not bool:
            raise ArgumentError(
                f"job {self.name!r} arg {key!r}: expected {expected_type.__name__}, got bool"
            )
        if expected_type is float and isinstance(value, int):
            return float(value)
        if not isinstance(value, expected_type):
            raise ArgumentError(
                f"job {self.name!r} arg {key!r}: expected {expected_type.__name__}, got {type(value).__name__}"
            )
        return value

    def to_wire(self) -> str:
        payload: dict[str, Any] = {
            "name": self.name,
            "id": self.id,
            "t": self.enqueued_at,
            "args": self.args,
        }
        if self.unique:
            payload["unique"] = True
        if self.fails:
            payload["fails"] = self.fails
        if self.last_err is not None:
            payload["err"] = self.last_err
        if self.failed_at is not None:
            payload["failed_at"] = self.failed_at
        return json.dumps(payload, separators=(",", ":"))
