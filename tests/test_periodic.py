"""Mirrors periodic_enqueuer_test.go: deterministic IDs, exact schedule
walking over the 4-minute horizon, byte-identical dedup, and shouldEnqueue.

Upstream freezes the clock and compares a literal expected table; here the
expected set is computed with the same Schedule objects the enqueuer uses,
anchored to a call that provably ran within one clock second, which keeps
the assertion exact without mocking time."""

import json
import time
from datetime import datetime, timedelta

from helpers import NAMESPACE
from pygowork import Job, PeriodicJob, WorkerPool

SPECS = [
    PeriodicJob(spec="0/29 * * * * *", job_name="foo"),  # seconds 0 and 29
    PeriodicJob(spec="3/49 * * * * *", job_name="bar"),  # seconds 3 and 52
    PeriodicJob(spec="* * * 2 * *", job_name="baz"),  # every second on the 2nd of the month
]


def noop(job: Job) -> None:
    return None


def enqueue_twice_within_one_second(pool: WorkerPool, redis_client) -> int:
    """Run _enqueue_periodic twice inside a single clock second (retrying
    until that provably happened) so the expected set can be computed from
    the anchor and the double call proves byte-identical dedup."""
    for _ in range(20):
        redis_client.delete(f"{NAMESPACE}:scheduled", f"{NAMESPACE}:last_periodic_enqueue")
        anchor = int(time.time())
        pool._enqueue_periodic()
        pool._enqueue_periodic()
        if int(time.time()) == anchor:
            return anchor
        time.sleep(0.05)
    raise AssertionError("could not run _enqueue_periodic within a single clock second")


def test_periodic_enqueue_is_deterministic(redis_client):
    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"foo": noop, "bar": noop, "baz": noop},
        requeuer=False,
        reaper=False,
        periodic=SPECS,
    )
    anchor = enqueue_twice_within_one_second(pool, redis_client)

    expected = set()
    horizon = datetime.fromtimestamp(anchor) + timedelta(seconds=240)
    for periodic in SPECS:
        moment = periodic.schedule.next_after(datetime.fromtimestamp(anchor))
        while moment < horizon:
            epoch = int(moment.timestamp())
            expected.add((f"periodic:{periodic.job_name}:{periodic.spec}:{epoch}", epoch))
            moment = periodic.schedule.next_after(moment)

    rows = redis_client.zrange(f"{NAMESPACE}:scheduled", 0, -1, withscores=True)
    actual = set()
    for raw, score in rows:
        payload = json.loads(raw)
        assert payload["args"] is None
        assert payload["t"] == int(score)  # enqueued_at equals the scheduled epoch
        actual.add((payload["id"], int(score)))

    # Exact match also proves the double call enqueued nothing twice:
    # identical bytes collapse in the zset.
    assert actual == expected
    assert len(actual) > 0

    # last_periodic_enqueue was stamped with the enqueue time.
    assert int(redis_client.get(f"{NAMESPACE}:last_periodic_enqueue")) == anchor


def test_should_enqueue_periodic(redis_client):
    pool = WorkerPool(
        redis_client, NAMESPACE, {"foo": noop}, requeuer=False, reaper=False,
        periodic=[PeriodicJob(spec="0 * * * * *", job_name="foo")],
    )

    # No stamp yet: enqueue.
    assert pool._should_enqueue_periodic() is True

    # Fresh stamp: don't. (Upstream's check is sleep/time.Minute = 2 seconds,
    # a Go quirk pygowork replicates faithfully.)
    redis_client.set(f"{NAMESPACE}:last_periodic_enqueue", int(time.time()))
    assert pool._should_enqueue_periodic() is False

    # Stale stamp: enqueue again.
    redis_client.set(f"{NAMESPACE}:last_periodic_enqueue", int(time.time()) - 10)
    assert pool._should_enqueue_periodic() is True
