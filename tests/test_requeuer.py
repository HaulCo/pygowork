"""Mirrors requeuer_test.go: due scheduled jobs move to their queues with a
refreshed t, jobs the pool does not recognize are dead-lettered. Upstream
backdates via a clock mock; here due-ness comes from negative enqueue_in
delays, which upstream also uses (EnqueueIn(-9))."""

import time

from helpers import NAMESPACE, job_on_queue, job_on_zset, jobs_key, list_size, zset_size
from pygowork import Job, Producer, WorkerPool


def drain_requeue(pool: WorkerPool, zset: str) -> None:
    while pool._requeue_once(zset):
        pass


def noop(job: Job) -> None:
    return None


def test_requeue(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue_in("wat", -9, None)
    producer.enqueue_in("wat", -9, None)
    producer.enqueue_in("foo", -1, None)
    producer.enqueue_in("foo", 3600, None)
    producer.enqueue_in("bar", 3600, None)

    pool = WorkerPool(
        redis_client, NAMESPACE, {"wat": noop, "foo": noop, "bar": noop}, requeuer=False, reaper=False
    )
    drain_requeue(pool, f"{NAMESPACE}:scheduled")

    assert list_size(redis_client, jobs_key("wat")) == 2
    assert list_size(redis_client, jobs_key("foo")) == 1
    assert list_size(redis_client, jobs_key("bar")) == 0
    assert zset_size(redis_client, f"{NAMESPACE}:scheduled") == 2

    # The job was scheduled with t in the past; requeueing must reset t to now.
    requeued = job_on_queue(redis_client, jobs_key("foo"))
    assert requeued.name == "foo"
    assert requeued.enqueued_at + 2 >= time.time()


def test_requeue_unknown(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue_in("wat", -9, None)

    pool = WorkerPool(redis_client, NAMESPACE, {"bar": noop}, requeuer=False, reaper=False)
    drain_requeue(pool, f"{NAMESPACE}:scheduled")

    assert zset_size(redis_client, f"{NAMESPACE}:scheduled") == 0
    assert zset_size(redis_client, f"{NAMESPACE}:dead") == 1

    score, job = job_on_zset(redis_client, f"{NAMESPACE}:dead")
    assert abs(score - time.time()) <= 2
    assert job.failed_at == score
    assert job.last_err == "unknown job when requeueing"
