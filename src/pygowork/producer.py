"""Enqueue jobs that gocraft/work worker pools consume natively.

Formats verified against gocraft/work v0.5.1 (job.go, enqueue.go, redis.go);
see job.py for the wire format. Go enqueuers maintain {namespace}:known_jobs
(confirmed against a live deployment), so enqueue mirrors that, including
addToKnownJobs' in-memory cache: the SADD happens at most once per five
minutes per job name per producer, which also means a known_jobs entry
deleted out from under a long-lived producer is not re-added until the
cache expires, exactly as in Go. Unique variants use gocraft's own Lua and
a unique key built exactly the way Go builds it: sorted-key compact JSON of
args plus a trailing newline, matching Go's json.Encoder output.
"""

import json
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any

from redis import Redis

from pygowork.job import Job
from pygowork.lua import ENQUEUE_UNIQUE, ENQUEUE_UNIQUE_IN


@dataclass
class ScheduledJob:
    run_at: int
    job: Job


def unique_job_key(namespace: str, job_name: str, args: dict[str, Any] | None) -> str:
    key = f"{namespace}:unique:{job_name}:"
    if args is not None:
        key += json.dumps(args, sort_keys=True, separators=(",", ":")) + "\n"
    return key


KNOWN_JOBS_CACHE_SECONDS = 300


class Producer:
    def __init__(self, redis_client: Redis, namespace: str) -> None:
        self.redis = redis_client
        self.namespace = namespace
        self._known_jobs: dict[str, int] = {}
        self._known_jobs_lock = threading.Lock()

    def _add_to_known_jobs(self, job_name: str) -> None:
        now = int(time.time())
        with self._known_jobs_lock:
            cached_until = self._known_jobs.get(job_name)
            if cached_until is not None and now < cached_until:
                return
        self.redis.sadd(f"{self.namespace}:known_jobs", job_name)
        with self._known_jobs_lock:
            self._known_jobs[job_name] = now + KNOWN_JOBS_CACHE_SECONDS

    def _new_job(
        self, job_name: str, args: dict[str, Any] | None, unique: bool = False
    ) -> Job:
        return Job(
            name=job_name,
            id=secrets.token_hex(6),
            enqueued_at=int(time.time()),
            args=args,
            unique=unique,
        )

    def enqueue(self, job_name: str, args: dict[str, Any] | None) -> Job:
        job = self._new_job(job_name, args)
        self.redis.lpush(f"{self.namespace}:jobs:{job_name}", job.to_wire())
        self._add_to_known_jobs(job_name)
        return job

    def enqueue_in(
        self, job_name: str, seconds_from_now: int, args: dict[str, Any] | None
    ) -> ScheduledJob:
        job = self._new_job(job_name, args)
        run_at = int(time.time()) + seconds_from_now
        self.redis.zadd(f"{self.namespace}:scheduled", {job.to_wire(): run_at})
        self._add_to_known_jobs(job_name)
        return ScheduledJob(run_at=run_at, job=job)

    def enqueue_unique(self, job_name: str, args: dict[str, Any] | None) -> Job | None:
        """Returns the job, or None when an identical job is already queued
        (uniqueness holds for 24 hours, matching gocraft)."""
        job = self._new_job(job_name, args, unique=True)
        result = self.redis.eval(
            ENQUEUE_UNIQUE,
            2,
            f"{self.namespace}:jobs:{job_name}",
            unique_job_key(self.namespace, job_name, args),
            job.to_wire(),
        )
        self._add_to_known_jobs(job_name)
        return job if result == b"ok" else None

    def enqueue_unique_in(
        self, job_name: str, seconds_from_now: int, args: dict[str, Any] | None
    ) -> ScheduledJob | None:
        job = self._new_job(job_name, args, unique=True)
        run_at = int(time.time()) + seconds_from_now
        result = self.redis.eval(
            ENQUEUE_UNIQUE_IN,
            2,
            f"{self.namespace}:scheduled",
            unique_job_key(self.namespace, job_name, args),
            job.to_wire(),
            run_at,
        )
        self._add_to_known_jobs(job_name)
        return ScheduledJob(run_at=run_at, job=job) if result == b"ok" else None
