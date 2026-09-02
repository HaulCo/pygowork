"""Mirrors dead_pool_reaper_test.go: finding dead pools, requeueing their
in-progress jobs, the no-heartbeat path (locks cleaned via the reaping
pool's own job types, jobs untouched), pools without job_names, and stale
lock cleaning with the floor at zero."""

import time

from redis import Redis

from helpers import NAMESPACE, get_int, hget_int, jobs_key, list_size
from pygowork import Job, WorkerPool


def noop(job: Job) -> None:
    return None


def stage_pool(
    redis_client: Redis,
    pool_id: str,
    heartbeat_at: int | None = None,
    job_names: str | None = None,
) -> None:
    redis_client.sadd(f"{NAMESPACE}:worker_pools", pool_id)
    mapping = {}
    if heartbeat_at is not None:
        mapping["heartbeat_at"] = heartbeat_at
    if job_names is not None:
        mapping["job_names"] = job_names
    if mapping:
        redis_client.hset(f"{NAMESPACE}:worker_pools:{pool_id}", mapping=mapping)


def test_reaper_requeues_dead_pools(redis_client):
    now = int(time.time())
    stage_pool(redis_client, "1", heartbeat_at=now, job_names="type1,type2")
    stage_pool(redis_client, "2", heartbeat_at=now - 3600, job_names="type1,type2")
    stage_pool(redis_client, "3", heartbeat_at=now - 3600, job_names="type1,type2")

    reaper = WorkerPool(redis_client, NAMESPACE, {}, requeuer=False, reaper=False)
    assert reaper._find_dead_pools() == {
        "2": ["type1", "type2"],
        "3": ["type1", "type2"],
    }

    # Pool 2 died with a job in progress and a lock held.
    redis_client.lpush(f"{jobs_key('type1')}:2:inprogress", "foo")
    redis_client.incr(f"{jobs_key('type1')}:lock")
    redis_client.hincrby(f"{jobs_key('type1')}:lock_info", "2", 1)
    assert list_size(redis_client, jobs_key("type1")) == 0
    assert list_size(redis_client, f"{jobs_key('type1')}:2:inprogress") == 1

    reaper._reap()

    # The in-progress job went back to its queue, and the locks were cleaned.
    assert list_size(redis_client, jobs_key("type1")) == 1
    assert list_size(redis_client, f"{jobs_key('type1')}:2:inprogress") == 0
    assert get_int(redis_client, f"{jobs_key('type1')}:lock") == 0
    assert redis_client.hget(f"{jobs_key('type1')}:lock_info", "2") is None

    # Dead pools were deregistered; the live one was left alone.
    assert not redis_client.sismember(f"{NAMESPACE}:worker_pools", "2")
    assert not redis_client.sismember(f"{NAMESPACE}:worker_pools", "3")
    assert redis_client.sismember(f"{NAMESPACE}:worker_pools", "1")


def test_reaper_no_heartbeat(redis_client):
    """Pools registered with no heartbeat hash at all: their stale lock info
    is cleaned using the reaping pool's own job types, but their in-progress
    queues are left untouched (there is no job_names list to requeue from)."""
    for pool_id in ("1", "2", "3"):
        stage_pool(redis_client, pool_id)
    redis_client.set(f"{jobs_key('type1')}:lock", 3)
    for pool_id in ("1", "2", "3"):
        redis_client.hset(f"{jobs_key('type1')}:lock_info", pool_id, 1)
    redis_client.lpush(f"{jobs_key('type1')}:2:inprogress", "foo")

    reaper = WorkerPool(
        redis_client, NAMESPACE, {"type1": noop}, requeuer=False, reaper=False
    )
    assert reaper._find_dead_pools() == {"1": [], "2": [], "3": []}

    reaper._reap()

    # Jobs queue and in-progress queue were not altered.
    assert list_size(redis_client, jobs_key("type1")) == 0
    assert list_size(redis_client, f"{jobs_key('type1')}:2:inprogress") == 1

    # Dead pools were removed from the set and stale lock info cleaned.
    assert redis_client.scard(f"{NAMESPACE}:worker_pools") == 0
    assert get_int(redis_client, f"{jobs_key('type1')}:lock") == 0
    for pool_id in ("1", "2", "3"):
        assert redis_client.hget(f"{jobs_key('type1')}:lock_info", pool_id) is None


def test_reaper_skips_pool_without_job_names(redis_client):
    now = int(time.time())
    stage_pool(redis_client, "1", heartbeat_at=now - 3600)  # heartbeat but no job_names
    stage_pool(redis_client, "2", heartbeat_at=now - 3600, job_names="type1,type2")

    reaper = WorkerPool(redis_client, NAMESPACE, {}, requeuer=False, reaper=False)
    assert reaper._find_dead_pools() == {"2": ["type1", "type2"]}

    redis_client.lpush(f"{jobs_key('type1')}:1:inprogress", "foo")
    redis_client.lpush(f"{jobs_key('type1')}:2:inprogress", "foo")

    reaper._reap()

    # Pool 2 was requeued; pool 1 was not.
    assert list_size(redis_client, jobs_key("type1")) == 1
    assert list_size(redis_client, f"{jobs_key('type1')}:1:inprogress") == 1
    assert list_size(redis_client, f"{jobs_key('type1')}:2:inprogress") == 0


def test_clean_stale_locks(redis_client):
    reaper = WorkerPool(
        redis_client,
        NAMESPACE,
        {"type1": noop, "type2": noop},
        requeuer=False,
        reaper=False,
    )
    job_names = ["type1", "type2"]
    lock1, lock2 = f"{jobs_key('type1')}:lock", f"{jobs_key('type2')}:lock"
    lock_info1, lock_info2 = (
        f"{jobs_key('type1')}:lock_info",
        f"{jobs_key('type2')}:lock_info",
    )

    redis_client.set(lock1, 3)
    redis_client.set(lock2, 1)
    redis_client.hset(lock_info1, "1", 1)  # pool 1 holds 1 lock on type1
    redis_client.hset(lock_info1, "2", 2)  # pool 2 holds 2 locks on type1
    redis_client.hset(lock_info2, "2", 2)  # more claimed than held: floor at 0

    reaper._clean_stale_lock_info("1", job_names)
    assert get_int(redis_client, lock1) == 2  # decremented by pool 1's single claim
    assert get_int(redis_client, lock2) == 1  # unchanged
    assert redis_client.hget(lock_info1, "1") is None

    reaper._clean_stale_lock_info("2", job_names)
    assert get_int(redis_client, lock1) == 0
    assert get_int(redis_client, lock2) == 0  # clamped, never negative
    assert redis_client.hget(lock_info1, "2") is None
    assert redis_client.hget(lock_info2, "2") is None
