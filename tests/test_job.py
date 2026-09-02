"""Wire-format contract for Job, plus identifier_test.go.

Upstream's job_test.go mostly exercises Go's ArgString/ArgInt64/ArgBool/
ArgFloat64 coercion helpers, which pygowork deliberately does not carry:
Python handlers read job.args directly, already typed by JSON. What must
hold identically is the wire format (job.go serialize/newJob), covered
here."""

import json

import pytest

from pygowork import ArgumentError, Job
from pygowork.consumer import make_identifier


def test_round_trip_preserves_wire_bytes():
    raw = b'{"name":"wat","id":"abc123","t":100,"args":{"a":1,"b":"cool"}}'
    job = Job.from_wire(raw)
    assert job.name == "wat"
    assert job.id == "abc123"
    assert job.enqueued_at == 100
    assert job.args == {"a": 1, "b": "cool"}
    assert job.unique is False
    assert job.fails == 0
    assert job.last_err is None
    assert job.failed_at is None
    assert job.raw == raw
    assert job.to_wire().encode() == raw


def test_omitempty_fields_absent_until_set():
    job = Job(name="wat", id="abc", enqueued_at=5, args=None)
    payload = json.loads(job.to_wire())
    assert set(payload) == {"name", "id", "t", "args"}
    assert payload["args"] is None


def test_failure_fields_serialize_like_go():
    job = Job(name="wat", id="abc", enqueued_at=5, args={"a": 1}, fails=2, last_err="boom", failed_at=99)
    payload = json.loads(job.to_wire())
    assert payload["fails"] == 2
    assert payload["err"] == "boom"
    assert payload["failed_at"] == 99


def test_unique_flag_round_trip():
    job = Job(name="wat", id="abc", enqueued_at=5, args=None, unique=True)
    assert json.loads(job.to_wire())["unique"] is True
    assert Job.from_wire(job.to_wire().encode()).unique is True


def test_fetched_job_carries_queue_provenance():
    job = Job.from_wire(b'{"name":"wat","id":"abc","t":1,"args":null}', "src_queue", "inprog_queue")
    assert job.dequeued_from == "src_queue"
    assert job.in_progress_queue == "inprog_queue"


def test_make_identifier():
    identifier = make_identifier()
    assert len(identifier) >= 10  # upstream asserts a string of length 10 at least


def arg_job(args: dict | None) -> Job:
    return Job(name="send_email", id="abc", enqueued_at=1, args=args)


def test_arg_returns_typed_values():
    job = arg_job({"address": "jo@example.com", "count": 3, "rate": 1.5, "urgent": True})
    assert job.arg("address", str) == "jo@example.com"
    assert job.arg("count", int) == 3
    assert job.arg("rate", float) == 1.5
    assert job.arg("urgent", bool) is True


def test_arg_missing_key():
    job = arg_job({"address": "jo@example.com"})
    with pytest.raises(ArgumentError, match=r"job 'send_email' arg 'subject': missing"):
        job.arg("subject", str)


def test_arg_missing_when_args_is_none():
    with pytest.raises(ArgumentError, match=r"arg 'subject': missing"):
        arg_job(None).arg("subject", str)


def test_arg_wrong_type_names_both_types():
    job = arg_job({"address": 7})
    with pytest.raises(ArgumentError, match=r"job 'send_email' arg 'address': expected str, got int"):
        job.arg("address", str)


def test_arg_null_reads_as_nonetype():
    job = arg_job({"address": None})
    with pytest.raises(ArgumentError, match=r"expected str, got NoneType"):
        job.arg("address", str)


def test_arg_bool_never_passes_for_int():
    """bool subclasses int in Python; True must not satisfy an int arg."""
    job = arg_job({"count": True})
    with pytest.raises(ArgumentError, match=r"expected int, got bool"):
        job.arg("count", int)


def test_arg_int_passes_for_float():
    """Go marshals float64(1.0) as 1, which arrives here as an int; asking
    for a float must accept it, as float."""
    job = arg_job({"rate": 1})
    value = job.arg("rate", float)
    assert value == 1.0
    assert isinstance(value, float)
