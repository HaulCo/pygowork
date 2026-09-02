"""Mirrors worker_test.go: basics, in-progress bookkeeping, retry (default
and custom backoff), dead-letter (including SkipDead via JobOptions), and
paused queues. TestStop's unreachable-Redis case lives in
test_worker_pool.py."""

import threading
import time

from helpers import (
    NAMESPACE,
    get_int,
    hget_int,
    in_progress_key,
    job_on_zset,
    jobs_key,
    list_size,
    read_hash,
    running_pool,
    wait_until,
    zset_size,
)
from pygowork import Job, JobOptions, Producer, WorkerPool


def test_worker_basics(redis_client):
    recorded = {}

    def make_handler(name: str):
        def handler(job: Job) -> None:
            recorded[name] = job.args["a"]

        return handler

    handlers = {name: make_handler(name) for name in ("job1", "job2", "job3")}
    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", {"a": 1})
    producer.enqueue("job2", {"a": 2})
    producer.enqueue("job3", {"a": 3})

    pool = WorkerPool(redis_client, NAMESPACE, handlers, requeuer=False, reaper=False)
    pool.drain()

    # The jobs ran, with their arguments.
    assert recorded == {"job1": 1, "job2": 2, "job3": 3}

    # Nothing in retries or dead.
    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 0
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 0

    # Nothing in the queues or in-progress queues.
    for name in ("job1", "job2", "job3"):
        assert list_size(redis_client, jobs_key(name)) == 0
        assert list_size(redis_client, in_progress_key(pool, name)) == 0

    # Nothing in the worker status.
    assert read_hash(redis_client, f"{NAMESPACE}:worker:{pool.worker_ids[0]}") == {}


def test_worker_in_progress(redis_client):
    entered = threading.Event()
    release = threading.Event()

    def handler(job: Job) -> None:
        entered.set()
        assert release.wait(5)

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", {"a": 1})

    pool = WorkerPool(
        redis_client, NAMESPACE, {"job1": handler}, requeuer=False, reaper=False
    )
    with running_pool(pool):
        assert entered.wait(5)
        # Mid-flight: the job left the queue for the in-progress queue, with
        # the lock and lock_info accounted to this pool.
        assert list_size(redis_client, jobs_key("job1")) == 0
        assert list_size(redis_client, in_progress_key(pool, "job1")) == 1
        assert get_int(redis_client, f"{jobs_key('job1')}:lock") == 1
        assert (
            hget_int(redis_client, f"{jobs_key('job1')}:lock_info", pool.pool_id) == 1
        )

        observation = read_hash(
            redis_client, f"{NAMESPACE}:worker:{pool.worker_ids[0]}"
        )
        assert observation["job_name"] == "job1"
        assert observation["args"] == '{"a":1}'

        release.set()
        wait_until(lambda: list_size(redis_client, in_progress_key(pool, "job1")) == 0)

    # At this point, it should all be empty.
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert get_int(redis_client, f"{jobs_key('job1')}:lock") == 0
    assert hget_int(redis_client, f"{jobs_key('job1')}:lock_info", pool.pool_id) == 0
    assert read_hash(redis_client, f"{NAMESPACE}:worker:{pool.worker_ids[0]}") == {}


def test_worker_retry(redis_client):
    def handler(job: Job) -> None:
        raise RuntimeError("sorry kid")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", {"a": 1})

    pool = WorkerPool(
        redis_client, NAMESPACE, {"job1": handler}, requeuer=False, reaper=False
    )
    pool.drain()

    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 1
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 0
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0
    assert get_int(redis_client, f"{jobs_key('job1')}:lock") == 0
    assert hget_int(redis_client, f"{jobs_key('job1')}:lock_info", pool.pool_id) == 0

    score, job = job_on_zset(redis_client, f"{NAMESPACE}:retry")
    assert score > time.time()  # enqueued in the future
    assert score < time.time() + 80  # but under the first-failure backoff ceiling
    assert job.name == "job1"
    assert job.fails == 1
    assert job.last_err == "sorry kid"
    assert time.time() - job.failed_at <= 2


def test_worker_retry_with_custom_backoff(redis_client):
    """Mirrors TestWorkerRetryWithCustomBackoff: the job's own backoff
    calculator replaces gocraft's default curve, and is called once."""
    backoff_calls = []

    def handler(job: Job) -> None:
        raise RuntimeError("sorry kid")

    def custom_backoff(job: Job) -> int:
        backoff_calls.append(job.name)
        return 5  # always 5 seconds

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", {"a": 1})

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"job1": handler},
        options={"job1": JobOptions(backoff=custom_backoff)},
        requeuer=False,
        reaper=False,
    )
    pool.drain()

    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 1
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 0
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0

    score, job = job_on_zset(redis_client, f"{NAMESPACE}:retry")
    assert score > time.time()  # enqueued in the future
    assert score < time.time() + 10  # but under ten seconds out
    assert job.name == "job1"
    assert job.fails == 1
    assert job.last_err == "sorry kid"
    assert time.time() - job.failed_at <= 2
    assert backoff_calls == ["job1"]


def test_worker_dead_with_skip_dead(redis_client):
    """Mirrors TestWorkerDead in full: job1 exhausts its fails into the dead
    set, job2 with skip_dead vanishes entirely. The per-job max_fails of 1
    also overrides the pool-wide default of 4."""

    def fail1(job: Job) -> None:
        raise RuntimeError("sorry kid1")

    def fail2(job: Job) -> None:
        raise RuntimeError("sorry kid2")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", None)
    producer.enqueue("job2", None)

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"job1": fail1, "job2": fail2},
        options={
            "job1": JobOptions(max_fails=1),
            "job2": JobOptions(max_fails=1, skip_dead=True),
        },
        requeuer=False,
        reaper=False,
    )
    pool.drain()

    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 0
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 1

    for name in ("job1", "job2"):
        assert list_size(redis_client, jobs_key(name)) == 0
        assert list_size(redis_client, in_progress_key(pool, name)) == 0
        assert get_int(redis_client, f"{jobs_key(name)}:lock") == 0
        assert hget_int(redis_client, f"{jobs_key(name)}:lock_info", pool.pool_id) == 0

    score, job = job_on_zset(redis_client, f"{NAMESPACE}:dead")
    assert score <= time.time()
    assert job.name == "job1"  # job2 was dropped, not dead-lettered
    assert job.fails == 1
    assert job.last_err == "sorry kid1"


def test_worker_dead(redis_client):
    def handler(job: Job) -> None:
        raise RuntimeError("sorry kid1")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", None)

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"job1": handler},
        max_fails=1,
        requeuer=False,
        reaper=False,
    )
    pool.drain()

    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 0
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 1
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0
    assert get_int(redis_client, f"{jobs_key('job1')}:lock") == 0
    assert hget_int(redis_client, f"{jobs_key('job1')}:lock_info", pool.pool_id) == 0

    score, job = job_on_zset(redis_client, f"{NAMESPACE}:dead")
    assert score <= time.time()
    assert job.name == "job1"
    assert job.fails == 1
    assert job.last_err == "sorry kid1"
    assert time.time() - job.failed_at <= 2


def test_argument_error_lands_in_dead_letter(redis_client):
    """The point of job.arg: a malformed payload fails the job with a
    message that names the job, the key, and both types, readable straight
    off the dead set."""

    def handler(job: Job) -> None:
        job.arg("address", str)

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("send_email", {"address": 7})

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"send_email": handler},
        max_fails=1,
        requeuer=False,
        reaper=False,
    )
    pool.drain()

    _, dead = job_on_zset(redis_client, f"{NAMESPACE}:dead")
    assert dead.last_err == "job 'send_email' arg 'address': expected str, got int"


def test_workers_paused(redis_client):
    processed = []

    def handler(job: Job) -> None:
        processed.append(job.id)

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("job1", {"a": 1})

    redis_client.set(f"{jobs_key('job1')}:paused", "1")
    pool = WorkerPool(
        redis_client, NAMESPACE, {"job1": handler}, requeuer=False, reaper=False
    )

    # Paused: the fetch Lua refuses the queue, so drain returns with the job
    # still queued and untouched.
    pool.drain()
    assert processed == []
    assert list_size(redis_client, jobs_key("job1")) == 1
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0

    # Unpaused: the same job processes.
    redis_client.delete(f"{jobs_key('job1')}:paused")
    pool.drain()
    assert len(processed) == 1
    assert list_size(redis_client, jobs_key("job1")) == 0
    assert list_size(redis_client, in_progress_key(pool, "job1")) == 0
