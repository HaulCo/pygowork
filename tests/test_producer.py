"""Mirrors enqueue_test.go: Enqueue, EnqueueIn, EnqueueUnique,
EnqueueUniqueIn, including the unique-signature lifecycle proven through a
real pool the way upstream does."""

import time

from redis import Redis

from helpers import NAMESPACE, job_on_queue, job_on_zset, jobs_key, list_size, zset_size
from pygowork import Job, Producer, WorkerPool


def known_jobs(redis_client: Redis) -> list[str]:
    return sorted(
        member.decode() for member in redis_client.smembers(f"{NAMESPACE}:known_jobs")
    )


def assert_fresh_job(job: Job, args: dict | None) -> None:
    assert len(job.id) > 10  # something is in it
    assert abs(job.enqueued_at - time.time()) < 10  # within 10 seconds
    assert job.args == args


def test_enqueue(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    job = producer.enqueue("wat", {"a": 1, "b": "cool"})
    assert job.name == "wat"
    assert_fresh_job(job, {"a": 1, "b": "cool"})

    assert known_jobs(redis_client) == ["wat"]
    assert list_size(redis_client, jobs_key("wat")) == 1

    queued = job_on_queue(redis_client, jobs_key("wat"))
    assert queued.name == "wat"
    assert_fresh_job(queued, {"a": 1, "b": "cool"})

    producer.enqueue("wat", {"a": 1, "b": "cool"})
    producer.enqueue("wat", {"a": 1, "b": "cool"})
    assert list_size(redis_client, jobs_key("wat")) == 2


def test_known_jobs_sadd_is_cached(redis_client):
    """Mirrors addToKnownJobs: one SADD per job name per five minutes per
    producer, tracked in an in-memory cache. Proven without mocks: delete
    the set inside the cache window and an enqueue does not re-add it; a
    fresh producer (cold cache) does. Poking the cache to an expired value
    is upstream's own move (enqueue_test.go sets knownJobs["wat"] = 4)."""
    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("wat", None)
    assert known_jobs(redis_client) == ["wat"]
    assert producer._known_jobs["wat"] > time.time() + 290  # the cache is set

    redis_client.delete(f"{NAMESPACE}:known_jobs")
    producer.enqueue("wat", None)  # inside the cache window: no re-add
    assert known_jobs(redis_client) == []

    producer.enqueue("taw", None)  # the cache is per name: a new name still adds
    assert known_jobs(redis_client) == ["taw"]

    fresh_producer = Producer(redis_client, NAMESPACE)
    fresh_producer.enqueue("wat", None)  # its own cache is cold
    assert known_jobs(redis_client) == ["taw", "wat"]

    redis_client.delete(f"{NAMESPACE}:known_jobs")
    producer._known_jobs["wat"] = 4  # expired long ago
    producer.enqueue("wat", None)
    assert known_jobs(redis_client) == ["wat"]  # expired entry re-adds
    assert producer._known_jobs["wat"] > time.time() + 290  # and refreshes the cache


def test_enqueue_in(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    scheduled = producer.enqueue_in("wat", 300, {"a": 1, "b": "cool"})
    assert scheduled.job.name == "wat"
    assert_fresh_job(scheduled.job, {"a": 1, "b": "cool"})
    assert scheduled.run_at == scheduled.job.enqueued_at + 300

    assert known_jobs(redis_client) == ["wat"]
    assert zset_size(redis_client, f"{NAMESPACE}:scheduled") == 1

    score, queued = job_on_zset(redis_client, f"{NAMESPACE}:scheduled")
    assert time.time() + 290 < score <= time.time() + 300
    assert queued.name == "wat"
    assert_fresh_job(queued, {"a": 1, "b": "cool"})


def test_enqueue_unique(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    job = producer.enqueue_unique("wat", {"a": 1, "b": "cool"})
    assert job is not None
    assert_fresh_job(job, {"a": 1, "b": "cool"})

    assert producer.enqueue_unique("wat", {"a": 1, "b": "cool"}) is None  # duplicate
    assert (
        producer.enqueue_unique("wat", {"a": 1, "b": "coolio"}) is not None
    )  # different args
    assert (
        producer.enqueue_unique("wat", None) is not None
    )  # nil args are their own signature
    assert producer.enqueue_unique("wat", None) is None  # duplicate nil args
    assert producer.enqueue_unique("taw", None) is not None  # different name

    # Process the queues; ensure the right number of jobs ran (dupes were
    # rejected) and that processing clears the unique signatures even when
    # the handler fails, exactly as upstream asserts.
    counts = {"wat": 0, "taw": 0}

    def count_wat(job: Job) -> None:
        counts["wat"] += 1

    def count_taw_and_fail(job: Job) -> None:
        counts["taw"] += 1
        raise RuntimeError("ohno")

    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"wat": count_wat, "taw": count_taw_and_fail},
        max_fails=1,
        requeuer=False,
        reaper=False,
    )
    pool.drain()
    assert counts == {"wat": 3, "taw": 1}

    # Enqueue again: all signatures were cleared by processing.
    assert producer.enqueue_unique("wat", {"a": 1, "b": "cool"}) is not None
    assert producer.enqueue_unique("wat", {"a": 1, "b": "coolio"}) is not None
    assert producer.enqueue_unique("taw", None) is not None  # even though taw failed


def test_enqueue_unique_in(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    scheduled = producer.enqueue_unique_in("wat", 300, {"a": 1, "b": "cool"})
    assert scheduled is not None
    assert_fresh_job(scheduled.job, {"a": 1, "b": "cool"})
    assert scheduled.run_at == scheduled.job.enqueued_at + 300

    # A duplicate at a different delay is rejected and must not move the
    # original's run time.
    assert producer.enqueue_unique_in("wat", 10, {"a": 1, "b": "cool"}) is None
    score, queued = job_on_zset(redis_client, f"{NAMESPACE}:scheduled")
    assert time.time() + 290 < score <= time.time() + 300
    assert queued.name == "wat"
    assert queued.unique is True  # the unique flag rides the wire

    assert producer.enqueue_unique_in("wat", 300, {"a": 1, "b": "coolio"}) is not None
    assert producer.enqueue_unique_in("wat", 300, None) is not None
    assert producer.enqueue_unique_in("wat", 300, None) is None
    assert producer.enqueue_unique_in("taw", 300, None) is not None
