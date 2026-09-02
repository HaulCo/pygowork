"""Mirrors heartbeater_test.go: membership in the worker_pools set, every
heartbeat hash field, and clean removal. Also covers what upstream handles
inside its heartbeater's error logging: a heartbeat that raises must not
kill the heartbeat thread, or a live pool would get reaped and its
in-flight jobs run twice."""

import time

from helpers import NAMESPACE, read_hash, running_pool, wait_until
from pygowork import Job, WorkerPool


def noop(job: Job) -> None:
    return None


def test_heartbeat(redis_client):
    pool = WorkerPool(redis_client, NAMESPACE, {"foo": noop, "bar": noop}, concurrency=10)
    before = int(time.time())
    pool.started_at = before
    pool._heartbeat()

    assert redis_client.sismember(f"{NAMESPACE}:worker_pools", pool.pool_id)

    beat = read_hash(redis_client, f"{NAMESPACE}:worker_pools:{pool.pool_id}")
    assert int(beat["heartbeat_at"]) >= before
    assert beat["started_at"] == str(before)
    assert beat["job_names"] == "bar,foo"  # sorted, comma-joined
    assert beat["worker_ids"] == ",".join(sorted(pool.worker_ids))
    assert beat["concurrency"] == "10"
    assert beat["pid"] != ""
    assert beat["host"] != ""

    pool._remove_heartbeat()
    assert not redis_client.sismember(f"{NAMESPACE}:worker_pools", pool.pool_id)
    assert read_hash(redis_client, f"{NAMESPACE}:worker_pools:{pool.pool_id}") == {}


class FlakyHeartbeatPool(WorkerPool):
    """Failure injection by subclass, not by mock: the startup beat succeeds
    (startup deliberately fails fast, and another test relies on that), the
    next two loop beats raise, then real heartbeats resume."""

    beats_seen = 0

    def _heartbeat(self) -> None:
        self.beats_seen += 1
        if self.beats_seen in (2, 3):
            raise ConnectionError("injected heartbeat failure")
        super()._heartbeat()


def test_heartbeat_loop_survives_failures(redis_client):
    pool = FlakyHeartbeatPool(
        redis_client, NAMESPACE, {"foo": noop},
        heartbeat_period=0.05, requeuer=False, reaper=False,
    )
    with running_pool(pool):
        wait_until(lambda: redis_client.sismember(f"{NAMESPACE}:worker_pools", pool.pool_id))
        first_beat = int(read_hash(redis_client, f"{NAMESPACE}:worker_pools:{pool.pool_id}")["heartbeat_at"])
        # The loop must outlive the two raising beats and keep beating:
        # heartbeat_at moves past the startup beat's stamp.
        wait_until(
            lambda: int(
                read_hash(redis_client, f"{NAMESPACE}:worker_pools:{pool.pool_id}")["heartbeat_at"]
            ) > first_beat,
            timeout=10,
        )
        assert pool.beats_seen > 3  # the raising beats did not end the loop
