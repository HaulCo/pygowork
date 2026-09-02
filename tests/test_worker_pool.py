"""Mirrors worker_pool_test.go and worker_test.go's pool-level cases:
stop-while-processing (issue #24), MaxConcurrency, and stopping against an
unreachable Redis.

Not carried from upstream: handler/middleware signature validation via
reflection (Python handlers are plain callables; there is nothing to
validate) and repeated Start/Start/Stop/Stop idempotence (run() is a
single-use blocking call by design)."""

import os
import signal
import threading
import time

from redis import Redis

from helpers import (
    NAMESPACE,
    get_int,
    hget_int,
    in_progress_key,
    jobs_key,
    list_size,
    running_pool,
    wait_until,
)
from pygowork import Job, JobOptions, Producer, WorkerPool


def test_stop_finishes_in_flight_jobs(redis_client):
    """Stop while jobs are still queued: in-flight jobs run to completion
    (never killed mid-handler) and the queue is not drained first."""
    counters = {"started": 0, "stopped": 0}
    counters_lock = threading.Lock()
    total_jobs = 30

    def handler(job: Job) -> None:
        with counters_lock:
            counters["started"] += 1
        time.sleep(0.05)
        with counters_lock:
            counters["stopped"] += 1

    producer = Producer(redis_client, NAMESPACE)
    for _ in range(total_jobs):
        producer.enqueue("sample_job", {})

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"sample_job": handler},
        concurrency=2,
        requeuer=False,
        reaper=False,
    )
    with running_pool(pool):
        wait_until(lambda: counters["started"] >= 4)

    assert counters["started"] == counters["stopped"]
    assert counters["started"] < total_jobs
    assert list_size(redis_client, jobs_key("sample_job")) > 0


def test_unreachable_redis_does_not_hang():
    unreachable = Redis(port=63790, socket_connect_timeout=0.2, socket_timeout=0.2)
    pool = WorkerPool(unreachable, NAMESPACE, {"wat": lambda job: None})
    failures = []

    def run_and_capture() -> None:
        try:
            pool.run()
        except Exception as run_error:
            failures.append(run_error)

    thread = threading.Thread(target=run_and_capture, daemon=True)
    thread.start()
    pool.stop()
    thread.join(timeout=5)
    assert not thread.is_alive()
    # pygowork fails fast on an unreachable Redis rather than idling the way
    # Go's lazy connection pool allows; either way, stop() must not hang.
    assert failures


def test_handle_signals_stops_the_pool(redis_client):
    """run(handle_signals=True) from the main thread: a real SIGINT stops
    the pool cleanly, and the previous handlers are restored afterwards."""
    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    pool = WorkerPool(
        redis_client, NAMESPACE, {"wat": lambda job: None}, requeuer=False, reaper=False
    )
    interrupter = threading.Timer(0.3, lambda: os.kill(os.getpid(), signal.SIGINT))
    interrupter.start()
    try:
        pool.run(handle_signals=True)  # returns because the signal called stop()
    finally:
        interrupter.cancel()

    assert signal.getsignal(signal.SIGINT) is previous_sigint
    assert signal.getsignal(signal.SIGTERM) is previous_sigterm
    assert not redis_client.sismember(f"{NAMESPACE}:worker_pools", pool.pool_id)


def test_max_concurrency_single_threaded(redis_client):
    """JobOptions(max_concurrency=1) on a concurrency-3 pool: the pool
    writes the job's max_concurrency key at startup (as Go's
    writeConcurrencyControlsToRedis does) and the fetch Lua then never lets
    more than one job in flight."""
    state = {"current": 0, "max_seen": 0, "done": 0}
    state_lock = threading.Lock()

    def handler(job: Job) -> None:
        with state_lock:
            state["current"] += 1
            state["max_seen"] = max(state["max_seen"], state["current"])
        time.sleep(0.03)
        with state_lock:
            state["current"] -= 1
            state["done"] += 1

    producer = Producer(redis_client, NAMESPACE)
    total_jobs = 5
    for _ in range(total_jobs):
        producer.enqueue("job1", {"a": 1})

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"job1": handler},
        concurrency=3,
        options={"job1": JobOptions(max_concurrency=1)},
        requeuer=False,
        reaper=False,
    )
    with running_pool(pool):
        wait_until(lambda: state["done"] == total_jobs, timeout=15)
        assert (
            get_int(redis_client, f"{jobs_key('job1')}:max_concurrency") == 1
        )  # written at startup
        assert get_int(redis_client, f"{jobs_key('job1')}:lock") <= 1

    assert state["max_seen"] == 1

    # At this point it should all be empty.
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0
    assert get_int(redis_client, f"{jobs_key('job1')}:lock") == 0
    assert hget_int(redis_client, f"{jobs_key('job1')}:lock_info", pool.pool_id) == 0
