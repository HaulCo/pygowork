"""Mirrors client_test.go across the whole admin surface: heartbeats,
observations, queues with latency, the three zset pages, dead-job delete /
retry / retry-all (including the 10k batching case and the unknown-queue
dead-letter), scheduled and retry deletion, and the unique-signature
cleanup on scheduled deletion.

Where upstream mocks epoch seconds to fix latencies and enqueue times, the
same shapes are staged here by writing wire-format payloads with backdated
t values — real Redis, real bytes, no clock mock."""

import threading
import time

from helpers import (
    NAMESPACE,
    insert_dead_job,
    job_on_queue,
    job_on_zset,
    jobs_key,
    list_size,
    running_pool,
    wait_until,
    zset_size,
)
from pygowork import Client, Job, Producer, WorkerPool
from pygowork.consumer import make_identifier


def noop(job: Job) -> None:
    return None


def test_worker_pool_heartbeats(redis_client):
    pool1 = WorkerPool(redis_client, NAMESPACE, {"wat": noop, "bob": noop}, concurrency=10)
    pool2 = WorkerPool(redis_client, NAMESPACE, {"foo": noop, "bar": noop}, concurrency=11)
    pool1._heartbeat()
    pool2._heartbeat()

    client = Client(redis_client, NAMESPACE)
    heartbeats = client.worker_pool_heartbeats()
    assert len(heartbeats) == 2

    by_id = {beat.worker_pool_id: beat for beat in heartbeats}
    beat1, beat2 = by_id[pool1.pool_id], by_id[pool2.pool_id]

    assert beat1.concurrency == 10
    assert beat1.job_names == ["bob", "wat"]
    assert beat1.worker_ids == sorted(pool1.worker_ids)

    assert beat2.concurrency == 11
    assert beat2.job_names == ["bar", "foo"]
    assert beat2.worker_ids == sorted(pool2.worker_ids)

    pool1._remove_heartbeat()
    pool2._remove_heartbeat()
    assert client.worker_pool_heartbeats() == []


def test_worker_observations(redis_client):
    entered = {"count": 0}
    entered_lock = threading.Lock()
    release = threading.Event()

    def slow(job: Job) -> None:
        with entered_lock:
            entered["count"] += 1
        assert release.wait(5)

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("wat", {"a": 1, "b": 2})
    producer.enqueue("foo", {"a": 3, "b": 4})

    pool = WorkerPool(
        redis_client, NAMESPACE, {"wat": slow, "foo": slow}, concurrency=10, requeuer=False, reaper=False
    )
    client = Client(redis_client, NAMESPACE)
    with running_pool(pool):
        wait_until(lambda: entered["count"] == 2)

        observations = client.worker_observations()
        assert len(observations) == 10

        busy_by_job = {}
        for observation in observations:
            assert observation.worker_id != ""
            if observation.is_busy:
                busy_by_job[observation.job_name] = observation
                assert observation.job_id
                assert time.time() - observation.started_at <= 3
            else:
                assert observation.job_name is None

        assert set(busy_by_job) == {"wat", "foo"}
        assert busy_by_job["wat"].args_json == '{"a":1,"b":2}'
        assert busy_by_job["foo"].args_json == '{"a":3,"b":4}'

        release.set()
        wait_until(lambda: not any(row.is_busy for row in client.worker_observations()))

    # Pool stopped: heartbeat gone, so no observations at all.
    assert client.worker_observations() == []


def test_queues(redis_client):
    redis_client.sadd(f"{NAMESPACE}:known_jobs", "wat", "foo", "zaz")
    now = int(time.time())

    def stage(job_name: str, enqueued_at: int) -> None:
        job = Job(name=job_name, id=make_identifier(), enqueued_at=enqueued_at, args=None)
        redis_client.lpush(jobs_key(job_name), job.to_wire())

    stage("foo", now - 300)  # pushed first, so it sits at the tail: the oldest
    stage("foo", now - 200)
    stage("wat", now - 100)

    client = Client(redis_client, NAMESPACE)
    queues = client.queues()

    assert [queue.job_name for queue in queues] == ["foo", "wat", "zaz"]
    assert [queue.count for queue in queues] == [2, 1, 0]
    assert abs(queues[0].latency - 300) <= 3
    assert abs(queues[1].latency - 100) <= 3
    assert queues[2].latency == 0


def test_scheduled_jobs(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue_in("wat", 0, {"a": 1, "b": 2})
    producer.enqueue_in("zaz", 4, {"a": 3, "b": 4})
    producer.enqueue_in("foo", 2, {"a": 3, "b": 4})

    client = Client(redis_client, NAMESPACE)
    rows, count = client.scheduled_jobs(1)
    assert count == 3
    assert [row.job.name for row in rows] == ["wat", "foo", "zaz"]  # ordered by run_at
    for row, delay in zip(rows, [0, 2, 4]):
        assert abs(row.run_at - (row.job.enqueued_at + delay)) <= 1
        assert row.job.fails == 0
        assert row.job.last_err is None
        assert row.job.failed_at is None
    assert rows[0].job.args == {"a": 1, "b": 2}


def test_retry_jobs(redis_client):
    def fail(job: Job) -> None:
        raise RuntimeError("ohno")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("wat", {"a": 1, "b": 2})
    pool = WorkerPool(redis_client, NAMESPACE, {"wat": fail}, requeuer=False, reaper=False)
    pool.drain()

    client = Client(redis_client, NAMESPACE)
    rows, count = client.retry_jobs(1)
    assert count == 1
    row = rows[0]
    assert row.job.name == "wat"
    assert row.job.args == {"a": 1, "b": 2}
    assert row.job.fails == 1
    assert row.job.last_err == "ohno"
    assert time.time() - row.job.failed_at <= 3
    assert row.retry_at > time.time()  # backed off into the future


def test_dead_jobs_pagination_and_delete(redis_client):
    def fail(job: Job) -> None:
        raise RuntimeError("ohno")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("wat", {"a": 1, "b": 2})
    pool = WorkerPool(redis_client, NAMESPACE, {"wat": fail}, max_fails=1, requeuer=False, reaper=False)
    pool.drain()

    client = Client(redis_client, NAMESPACE)
    rows, count = client.dead_jobs(1)
    assert count == 1
    dead = rows[0]
    assert dead.job.name == "wat"
    assert dead.job.args == {"a": 1, "b": 2}
    assert dead.job.fails == 1
    assert dead.job.last_err == "ohno"
    assert time.time() - dead.job.failed_at <= 3

    # Pagination: page 2 is empty but the total still counts.
    rows, count = client.dead_jobs(2)
    assert rows == [] and count == 1

    assert client.delete_dead_job(dead.died_at, dead.job.id) is True
    rows, count = client.dead_jobs(1)
    assert rows == [] and count == 0


def test_delete_dead_jobs_one_by_one(redis_client):
    # Two of these share a died_at score; deletion must select by id.
    for failed_at in (12347, 12347, 12349, 12350):
        insert_dead_job(redis_client, "wat", 12345, failed_at)

    client = Client(redis_client, NAMESPACE)
    rows, count = client.dead_jobs(1)
    assert count == 4

    remaining = count
    for row in rows:
        assert client.delete_dead_job(row.died_at, row.job.id) is True
        _, remaining_now = client.dead_jobs(1)
        assert remaining_now == remaining - 1
        remaining -= 1


def test_retry_dead_job(redis_client):
    for name, failed_at in (("wat1", 12347), ("wat2", 12347), ("wat3", 12349), ("wat4", 12350)):
        insert_dead_job(redis_client, name, 12345, failed_at)

    client = Client(redis_client, NAMESPACE)
    rows, count = client.dead_jobs(1)
    assert count == 4

    remaining = count
    for row in rows:
        assert client.retry_dead_job(row.died_at, row.job.id) == 1
        _, remaining_now = client.dead_jobs(1)
        assert remaining_now == remaining - 1
        remaining -= 1

    # Every job is back on its queue with the failure history wiped.
    for name in ("wat1", "wat2", "wat3", "wat4"):
        revived = job_on_queue(redis_client, jobs_key(name))
        assert revived.name == name
        assert revived.fails == 0
        assert revived.last_err is None
        assert revived.failed_at is None
        assert time.time() - revived.enqueued_at <= 3  # t reset to now


def test_retry_dead_job_preserves_args(redis_client):
    job = Job(
        name="foobar",
        id=make_identifier(),
        enqueued_at=12345,
        args={"a": "wat"},
        fails=3,
        last_err="sorry",
        failed_at=12347,
    )
    redis_client.zadd(f"{NAMESPACE}:dead", {job.to_wire(): 12347})
    redis_client.sadd(f"{NAMESPACE}:known_jobs", "foobar")

    client = Client(redis_client, NAMESPACE)
    assert client.retry_dead_job(12347, job.id) == 1

    revived = job_on_queue(redis_client, jobs_key("foobar"))
    assert revived.name == "foobar"
    assert revived.args == {"a": "wat"}


def test_delete_all_dead_jobs(redis_client):
    for failed_at in (12347, 12347, 12349, 12350):
        insert_dead_job(redis_client, "wat", 12345, failed_at)

    client = Client(redis_client, NAMESPACE)
    _, count = client.dead_jobs(1)
    assert count == 4

    client.delete_all_dead_jobs()
    rows, count = client.dead_jobs(1)
    assert rows == [] and count == 0


def test_retry_all_dead_jobs(redis_client):
    for name in ("wat1", "wat2", "wat3", "wat4"):
        insert_dead_job(redis_client, name, 12345, 12347)

    client = Client(redis_client, NAMESPACE)
    assert client.retry_all_dead_jobs() == 4
    _, count = client.dead_jobs(1)
    assert count == 0

    for name in ("wat1", "wat2", "wat3", "wat4"):
        revived = job_on_queue(redis_client, jobs_key(name))
        assert revived.name == name
        assert revived.fails == 0
        assert revived.last_err is None
        assert revived.failed_at is None
        assert time.time() - revived.enqueued_at <= 3


def test_retry_all_dead_jobs_big(redis_client):
    """10,000 dead jobs exercise the 1000-per-batch requeue loop; one job
    with no known queue must be left behind, dead-lettered with
    'unknown job when requeueing'."""
    pipeline = redis_client.pipeline(transaction=False)
    for _ in range(10000):
        job = Job(
            name="wat1", id=make_identifier(), enqueued_at=12345, args=None,
            fails=3, last_err="sorry", failed_at=12347,
        )
        pipeline.zadd(f"{NAMESPACE}:dead", {job.to_wire(): 12347})
    pipeline.execute()
    redis_client.sadd(f"{NAMESPACE}:known_jobs", "wat1")

    unknown = Job(
        name="dontexist", id=make_identifier(), enqueued_at=12345, args=None,
        fails=3, last_err="sorry", failed_at=12347,
    )
    redis_client.zadd(f"{NAMESPACE}:dead", {unknown.to_wire(): 12347})

    client = Client(redis_client, NAMESPACE)
    _, count = client.dead_jobs(1)
    assert count == 10001

    assert client.retry_all_dead_jobs() == 10000
    _, count = client.dead_jobs(1)
    assert count == 1  # the funny job that we didn't know how to queue up
    assert list_size(redis_client, jobs_key("wat1")) == 10000

    _, leftover = job_on_zset(redis_client, f"{NAMESPACE}:dead")
    assert leftover.name == "dontexist"
    assert leftover.last_err == "unknown job when requeueing"


def test_delete_scheduled_job(redis_client):
    client = Client(redis_client, NAMESPACE)
    assert client.delete_scheduled_job(3, "bob") is False  # nothing there

    producer = Producer(redis_client, NAMESPACE)
    scheduled = producer.enqueue_in("foo", 10, None)
    assert client.delete_scheduled_job(scheduled.run_at, scheduled.job.id) is True
    assert zset_size(redis_client, f"{NAMESPACE}:scheduled") == 0


def test_delete_scheduled_unique_job_clears_signature(redis_client):
    producer = Producer(redis_client, NAMESPACE)
    scheduled = producer.enqueue_unique_in("foo", 10, None)
    assert scheduled is not None

    client = Client(redis_client, NAMESPACE)
    assert client.delete_scheduled_job(scheduled.run_at, scheduled.job.id) is True
    assert zset_size(redis_client, f"{NAMESPACE}:scheduled") == 0

    # Can do it again: deletion cleared the unique signature.
    assert producer.enqueue_unique_in("foo", 10, None) is not None


def test_delete_retry_job(redis_client):
    def fail(job: Job) -> None:
        raise RuntimeError("ohno")

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("wat", {"a": 1, "b": 2})
    pool = WorkerPool(redis_client, NAMESPACE, {"wat": fail}, requeuer=False, reaper=False)
    pool.drain()

    client = Client(redis_client, NAMESPACE)
    rows, count = client.retry_jobs(1)
    assert count == 1
    assert client.delete_retry_job(rows[0].retry_at, rows[0].job.id) is True
    assert zset_size(redis_client, f"{NAMESPACE}:retry") == 0
