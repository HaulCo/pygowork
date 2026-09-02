"""Shared helpers mirroring upstream's test harness (worker_test.go's
listSize/zsetSize/jobOnZset/readHash family and client_test.go's
insertDeadJob), plus a context manager that runs a pool on a thread."""

import threading
import time
from collections.abc import Callable
from contextlib import contextmanager

from redis import Redis

from pygowork import Job, WorkerPool
from pygowork.consumer import make_identifier

NAMESPACE = "pygowork_test"


def jobs_key(job_name: str) -> str:
    return f"{NAMESPACE}:jobs:{job_name}"


def in_progress_key(pool: WorkerPool, job_name: str) -> str:
    return f"{NAMESPACE}:jobs:{job_name}:{pool.pool_id}:inprogress"


def zset_size(redis_client: Redis, key: str) -> int:
    return redis_client.zcard(key)


def list_size(redis_client: Redis, key: str) -> int:
    return redis_client.llen(key)


def get_int(redis_client: Redis, key: str) -> int:
    value = redis_client.get(key)
    return int(value) if value is not None else 0


def hget_int(redis_client: Redis, key: str, field: str) -> int:
    value = redis_client.hget(key, field)
    return int(value) if value is not None else 0


def read_hash(redis_client: Redis, key: str) -> dict[str, str]:
    raw = redis_client.hgetall(key)
    return {field.decode(): value.decode() for field, value in raw.items()}


def job_on_zset(redis_client: Redis, key: str) -> tuple[int, Job]:
    rows = redis_client.zrange(key, 0, 0, withscores=True)
    assert rows, f"expected a job on zset {key}"
    raw, score = rows[0]
    return int(score), Job.from_wire(raw)


def job_on_queue(redis_client: Redis, key: str) -> Job:
    raw = redis_client.rpop(key)
    assert raw is not None, f"expected a job on queue {key}"
    return Job.from_wire(raw)


def insert_dead_job(
    redis_client: Redis, name: str, enqueued_at: int, failed_at: int
) -> Job:
    job = Job(
        name=name,
        id=make_identifier(),
        enqueued_at=enqueued_at,
        args=None,
        fails=3,
        last_err="sorry",
        failed_at=failed_at,
    )
    redis_client.zadd(f"{NAMESPACE}:dead", {job.to_wire(): failed_at})
    redis_client.sadd(f"{NAMESPACE}:known_jobs", name)
    return job


def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


@contextmanager
def running_pool(pool: WorkerPool):
    thread = threading.Thread(target=pool.run, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        pool.stop()
        thread.join(timeout=10)
        assert not thread.is_alive(), "pool thread failed to stop"
