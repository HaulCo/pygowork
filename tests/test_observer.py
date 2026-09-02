"""Mirrors observer_test.go: observeStarted, started-then-done,
observeCheckin, and Checkin from inside a job. Upstream drives the observer
struct directly; pygowork's observation writes are pool methods, driven the
same way here, with the from-inside-a-job case run through a real pool."""

import threading
import time

from helpers import NAMESPACE, read_hash, running_pool
from pygowork import Job, Producer, WorkerPool


def noop(job: Job) -> None:
    return None


def make_pool(redis_client) -> WorkerPool:
    return WorkerPool(redis_client, NAMESPACE, {"foo": noop}, requeuer=False, reaper=False)


def observation_job() -> Job:
    return Job(name="foo", id="bar", enqueued_at=1, args={"a": 1, "b": "wat"})


def test_observe_started(redis_client):
    pool = make_pool(redis_client)
    started_at = int(time.time())
    pool._observe_started("abcd", observation_job(), started_at)

    observation = read_hash(redis_client, f"{NAMESPACE}:worker:abcd")
    assert observation["job_name"] == "foo"
    assert observation["job_id"] == "bar"
    assert observation["started_at"] == str(started_at)
    assert observation["args"] == '{"a":1,"b":"wat"}'


def test_observe_started_then_done(redis_client):
    pool = make_pool(redis_client)
    pool._observe_started("abcd", observation_job(), int(time.time()))
    pool._observe_done("abcd")
    assert read_hash(redis_client, f"{NAMESPACE}:worker:abcd") == {}


def test_observe_checkin(redis_client):
    pool = make_pool(redis_client)
    started_at = int(time.time())
    pool._observe_started("abcd", observation_job(), started_at)

    pool._local.worker_id = "abcd"
    pool.checkin("doin it")

    observation = read_hash(redis_client, f"{NAMESPACE}:worker:abcd")
    assert observation["job_name"] == "foo"
    assert observation["job_id"] == "bar"
    assert observation["started_at"] == str(started_at)
    assert observation["args"] == '{"a":1,"b":"wat"}'
    assert observation["checkin"] == "doin it"
    assert abs(int(observation["checkin_at"]) - time.time()) <= 2


def test_checkin_from_job(redis_client):
    """job.checkin() inside a handler lands on the live observation,
    mirroring TestObserverCheckinFromJob through a real pool."""
    checked_in = threading.Event()
    release = threading.Event()

    def handler(job: Job) -> None:
        job.checkin("sup")
        checked_in.set()
        assert release.wait(5)

    producer = Producer(redis_client, NAMESPACE)
    producer.enqueue("foo", {"a": 1, "b": "wat"})

    pool = WorkerPool(redis_client, NAMESPACE, {"foo": handler}, requeuer=False, reaper=False)
    with running_pool(pool):
        assert checked_in.wait(5)
        observation = read_hash(redis_client, f"{NAMESPACE}:worker:{pool.worker_ids[0]}")
        assert observation["job_name"] == "foo"
        assert observation["args"] == '{"a":1,"b":"wat"}'
        assert observation["checkin"] == "sup"
        assert observation["checkin_at"] != ""
        release.set()


def test_checkin_outside_a_job_is_a_noop(redis_client):
    """pygowork-specific guard: checkin from a thread that is not running a
    job writes nothing and does not raise."""
    pool = make_pool(redis_client)
    pool.checkin("nope")
    assert read_hash(redis_client, f"{NAMESPACE}:worker:{pool.worker_ids[0]}") == {}
