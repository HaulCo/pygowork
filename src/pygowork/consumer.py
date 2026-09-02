"""Consume jobs the way a gocraft/work worker pool does, interoperably with Go
pools on the same namespace. Semantics verified against gocraft/work v0.5.1:
the multi-key atomics are gocraft's own Lua (see lua.py); completion, retry,
dead-letter, heartbeat, requeuer, dead-pool reaper, and identifier formats
mirror worker.go, heartbeater.go, requeuer.go, dead_pool_reaper.go, and
identifier.go. A pool heartbeating in this format is covered by any pool's
dead-pool reaper, Go or Python: crash mid-job and the fleet requeues it.

The requeuer hazard, inherited from gocraft: every pool's requeuer moves due
retry/scheduled jobs it recognizes and dead-letters ones it does not. On a
namespace where pools register different job subsets, each pool's requeuer
can dead-letter the other's due jobs. Either register every job name in the
namespace on every pool, or give heterogeneous pools separate namespaces.

Observations are written per job rather than batched on upstream's
one-second ticker; same keys and fields, only write frequency differs.
"""

import json
import logging
import os
import random
import secrets
import signal
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from redis import Redis

from pygowork.cron import Schedule
from pygowork.job import Job
from pygowork.lua import FETCH_JOB, REAP_STALE_LOCKS, REENQUEUE_JOB, ZREM_LPUSH
from pygowork.producer import unique_job_key

logger = logging.getLogger("pygowork")

HEARTBEAT_PERIOD_SECONDS = 5
DEFAULT_MAX_FAILS = 4
IDLE_BACKOFF_SECONDS = [0, 0.01, 0.1, 1, 5]
REQUEUE_PERIOD_SECONDS = 1
REAP_PERIOD_SECONDS = 600
REAP_JITTER_SECONDS = 30
DEAD_TIME_SECONDS = 10
PERIODIC_SLEEP_SECONDS = 120
PERIODIC_HORIZON_SECONDS = 240
OBSERVATION_TTL_SECONDS = 86400


def make_identifier() -> str:
    return secrets.token_hex(12)


def default_backoff_seconds(fails: int) -> int:
    return (fails**4) + 15 + random.randint(0, 29) * (fails + 1)


@dataclass
class PeriodicJob:
    """A periodic enqueue registration, mirroring gocraft's periodicJob:
    a robfig cron spec (see cron.py for accepted forms) and the job name."""

    spec: str
    job_name: str
    schedule: Schedule = field(init=False)

    def __post_init__(self) -> None:
        self.schedule = Schedule(self.spec)


@dataclass
class JobOptions:
    """Per-job-name configuration, mirroring gocraft's JobOptions.

    priority and max_fails left at None inherit the pool-wide setting (so
    passing JobOptions(skip_dead=True) never silently resets a priority
    configured elsewhere). max_concurrency 0 means unlimited, matching Go;
    the pool SETs the job's max_concurrency key for every registered job on
    startup, as Go's writeConcurrencyControlsToRedis does. backoff returns
    seconds until the next retry and replaces gocraft's default curve for
    this job; skip_dead drops a job that exhausts its fails instead of
    writing it to the dead set."""

    priority: int | None = None
    max_fails: int | None = None
    skip_dead: bool = False
    max_concurrency: int = 0
    backoff: Callable[[Job], int] | None = None


class WorkerPool:
    def __init__(
        self,
        redis_client: Redis,
        namespace: str,
        handlers: dict[str, Callable[[Job], None]],
        concurrency: int = 1,
        priorities: dict[str, int] | None = None,
        options: dict[str, JobOptions] | None = None,
        max_fails: int = DEFAULT_MAX_FAILS,
        requeuer: bool = True,
        reaper: bool = True,
        heartbeat_period: float = HEARTBEAT_PERIOD_SECONDS,
        requeue_period: float = REQUEUE_PERIOD_SECONDS,
        reap_period: float = REAP_PERIOD_SECONDS,
        dead_time: float = DEAD_TIME_SECONDS,
        periodic: list[PeriodicJob] | None = None,
        periodic_sleep: float = PERIODIC_SLEEP_SECONDS,
    ) -> None:
        self.redis = redis_client
        self.namespace = namespace
        self.handlers = handlers
        self.concurrency = concurrency
        self.options = {name: (options or {}).get(name, JobOptions()) for name in handlers}
        self.priorities = {
            name: self.options[name].priority
            if self.options[name].priority is not None
            else (priorities or {}).get(name, 1)
            for name in handlers
        }
        self.max_fails = max_fails
        self.run_requeuer = requeuer
        self.run_reaper = reaper
        self.heartbeat_period = heartbeat_period
        self.requeue_period = requeue_period
        self.reap_period = reap_period
        self.dead_time = dead_time
        self.periodic = list(periodic or [])
        self.periodic_sleep = periodic_sleep
        self.pool_id = make_identifier()
        self.worker_ids = [make_identifier() for _ in range(concurrency)]
        self._local = threading.local()
        self.started_at: int | None = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # -- key builders, mirroring redis.go --

    def _key(self, suffix: str) -> str:
        return f"{self.namespace}:{suffix}"

    def _jobs_key(self, job_name: str) -> str:
        return self._key(f"jobs:{job_name}")

    def _in_progress_key(self, job_name: str) -> str:
        return f"{self._jobs_key(job_name)}:{self.pool_id}:inprogress"

    # -- lifecycle --

    def run(self, handle_signals: bool = False) -> None:
        """Blocking; call stop() from another thread (or a signal handler) to
        exit. With handle_signals=True, SIGTERM and SIGINT call stop() so the
        pool finishes in-flight jobs and exits cleanly (only valid from the
        main thread, where Python allows signal registration); the previous
        handlers are restored on the way out."""
        previous_handlers = {}
        if handle_signals:
            def stop_on_signal(signal_number, frame) -> None:
                self.stop()

            for signal_number in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signal_number] = signal.signal(signal_number, stop_on_signal)
        self.started_at = int(time.time())
        self._write_concurrency_controls()
        self._heartbeat()
        self._spawn(self._heartbeat_loop)
        if self.run_requeuer:
            self._spawn(self._requeue_loop)
        if self.run_reaper:
            self._spawn(self._reap_loop)
        if self.periodic:
            self._spawn(self._periodic_loop)
        for worker_id in self.worker_ids[1:]:
            self._spawn(lambda wid=worker_id: self._work_loop(wid))
        logger.info(
            "worker pool started",
            extra={
                "pool_id": self.pool_id,
                "namespace": self.namespace,
                "jobs": sorted(self.handlers),
                "concurrency": self.concurrency,
            },
        )
        try:
            self._work_loop(self.worker_ids[0])
        finally:
            for thread in self._threads:
                thread.join(timeout=self.heartbeat_period + 1)
            self._remove_heartbeat()
            for signal_number, previous_handler in previous_handlers.items():
                signal.signal(signal_number, previous_handler)
            logger.info("worker pool stopped", extra={"pool_id": self.pool_id})

    def stop(self) -> None:
        self._stop.set()

    def drain(self) -> None:
        """Process jobs until every registered queue is empty, then return.
        Standalone one-shot alternative to run(); no background threads."""
        self.started_at = self.started_at or int(time.time())
        self._write_concurrency_controls()
        self._heartbeat()
        try:
            while True:
                fetched = self._fetch()
                if fetched is None:
                    return
                self._process(self.worker_ids[0], fetched)
        finally:
            self._remove_heartbeat()

    def _spawn(self, target: Callable[[], None]) -> None:
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        self._threads.append(thread)

    def _write_concurrency_controls(self) -> None:
        """Mirrors writeConcurrencyControlsToRedis: every registered job gets
        its max_concurrency key SET on startup, 0 meaning unlimited, which
        also overwrites stale values from earlier configurations."""
        if not self.handlers:
            return
        pipeline = self.redis.pipeline(transaction=False)
        for name in self.handlers:
            pipeline.set(f"{self._jobs_key(name)}:max_concurrency", self.options[name].max_concurrency)
        pipeline.execute()

    # -- worker loop / fetch / process / complete, mirroring worker.go --

    def _work_loop(self, worker_id: str) -> None:
        consecutive_no_jobs = 0
        while not self._stop.is_set():
            try:
                fetched = self._fetch()
            except Exception:
                logger.exception("fetch failed")
                self._stop.wait(0.01)
                continue
            if fetched is None:
                consecutive_no_jobs += 1
                index = min(consecutive_no_jobs, len(IDLE_BACKOFF_SECONDS) - 1)
                self._stop.wait(IDLE_BACKOFF_SECONDS[index])
                continue
            consecutive_no_jobs = 0
            try:
                self._process(worker_id, fetched)
            except Exception:
                # A Redis error while completing must not kill the worker
                # thread; Go logs and keeps looping. The job stays in the
                # in-progress queue, the same leak Go accepts here.
                logger.exception("processing failed", extra={"job": fetched.name, "job_id": fetched.id})

    def _sampled_job_names(self) -> list[str]:
        """Priority-weighted order without replacement, gocraft's sampling intent."""
        remaining = dict(self.priorities)
        ordered = []
        while remaining:
            names = list(remaining)
            weights = [remaining[name] for name in names]
            picked = random.choices(names, weights=weights)[0]
            ordered.append(picked)
            del remaining[picked]
        return ordered

    def _fetch(self) -> Job | None:
        keys = []
        for name in self._sampled_job_names():
            jobs = self._jobs_key(name)
            keys += [
                jobs,
                self._in_progress_key(name),
                f"{jobs}:paused",
                f"{jobs}:lock",
                f"{jobs}:lock_info",
                f"{jobs}:max_concurrency",
            ]
        result = self.redis.eval(FETCH_JOB, len(keys), *keys, self.pool_id)
        if result is None:
            return None
        raw, source_queue, in_progress_queue = result
        return Job.from_wire(raw, source_queue.decode(), in_progress_queue.decode())

    def _process(self, worker_id: str, job: Job) -> None:
        if job.unique:
            self.redis.delete(unique_job_key(self.namespace, job.name, job.args))
        started = time.time()
        self._observe_started(worker_id, job, int(started))
        self._local.worker_id, self._local.job_id = worker_id, job.id
        job.checkin = self.checkin
        run_error: Exception | None = None
        try:
            self.handlers[job.name](job)
        except Exception as handler_error:
            run_error = handler_error
            logger.exception("job failed", extra={"job": job.name, "job_id": job.id})
        else:
            logger.info(
                "job done",
                extra={
                    "job": job.name,
                    "job_id": job.id,
                    "elapsed_seconds": round(time.time() - started, 2),
                },
            )
        self._observe_done(worker_id)
        self._complete(job, run_error)

    # -- observations, mirroring observer.go's redis writes (written per job
    # rather than batched on a one-second ticker; same keys and fields) --

    def _observe_started(self, worker_id: str, job: Job, started_at: int) -> None:
        pipeline = self.redis.pipeline(transaction=False)
        pipeline.hset(
            self._key(f"worker:{worker_id}"),
            mapping={
                "job_name": job.name,
                "job_id": job.id,
                "started_at": started_at,
                "args": json.dumps(job.args, separators=(",", ":")) if job.args else "",
            },
        )
        pipeline.expire(self._key(f"worker:{worker_id}"), OBSERVATION_TTL_SECONDS)
        pipeline.execute()

    def _observe_done(self, worker_id: str) -> None:
        self.redis.delete(self._key(f"worker:{worker_id}"))

    def checkin(self, message: str) -> None:
        """Callable from inside a handler; mirrors gocraft's Job.Checkin."""
        worker_id = getattr(self._local, "worker_id", None)
        if worker_id is None:
            return
        pipeline = self.redis.pipeline(transaction=False)
        pipeline.hset(
            self._key(f"worker:{worker_id}"),
            mapping={"checkin": message, "checkin_at": int(time.time())},
        )
        pipeline.expire(self._key(f"worker:{worker_id}"), OBSERVATION_TTL_SECONDS)
        pipeline.execute()

    def _complete(self, job: Job, run_error: Exception | None) -> None:
        pipeline = self.redis.pipeline(transaction=True)
        pipeline.lrem(job.in_progress_queue, 1, job.raw)
        pipeline.decr(f"{self._jobs_key(job.name)}:lock")
        pipeline.hincrby(f"{self._jobs_key(job.name)}:lock_info", self.pool_id, -1)
        if run_error is not None:
            now = int(time.time())
            job_options = self.options[job.name]
            max_fails = job_options.max_fails if job_options.max_fails is not None else self.max_fails
            job.fails += 1
            job.last_err = str(run_error)
            job.failed_at = now
            if job.fails < max_fails:
                backoff_seconds = (
                    job_options.backoff(job)
                    if job_options.backoff is not None
                    else default_backoff_seconds(job.fails)
                )
                pipeline.zadd(self._key("retry"), {job.to_wire(): now + backoff_seconds})
            elif not job_options.skip_dead:
                pipeline.zadd(self._key("dead"), {job.to_wire(): now})
        pipeline.execute()

    # -- requeuer, mirroring requeuer.go --

    def _requeue_loop(self) -> None:
        while not self._stop.wait(self.requeue_period):
            for zset in (self._key("retry"), self._key("scheduled")):
                try:
                    while self._requeue_once(zset):
                        pass
                except Exception:
                    logger.exception("requeue failed", extra={"zset": zset})

    def _requeue_once(self, zset: str) -> bool:
        keys = [zset, self._key("dead")] + [self._jobs_key(name) for name in self.handlers]
        result = self.redis.eval(ZREM_LPUSH, len(keys), *keys, self._key("jobs:"), int(time.time()))
        if result == b"dead":
            logger.error("requeued job had no known queue; dead-lettered", extra={"zset": zset})
        return result in (b"ok", b"dead")

    # -- periodic enqueuer, mirroring periodic_enqueuer.go --

    def _periodic_loop(self) -> None:
        while True:
            try:
                if self._should_enqueue_periodic():
                    self._enqueue_periodic()
            except Exception:
                logger.exception("periodic enqueue failed", extra={"pool_id": self.pool_id})
            if self._stop.wait(self.periodic_sleep + random.randint(0, 30)):
                return

    def _should_enqueue_periodic(self) -> bool:
        last = self.redis.get(self._key("last_periodic_enqueue"))
        if last is None:
            return True
        # Faithful to upstream: the Go code compares against now minus
        # (sleep / time.Minute), which is 2 *seconds*, not 2 minutes.
        return int(last) < int(time.time()) - 2

    def _enqueue_periodic(self) -> None:
        now = int(time.time())
        now_moment = datetime.fromtimestamp(now)
        horizon = now_moment + timedelta(seconds=PERIODIC_HORIZON_SECONDS)
        pipeline = self.redis.pipeline(transaction=False)
        for periodic in self.periodic:
            moment = periodic.schedule.next_after(now_moment)
            while moment < horizon:
                epoch = int(moment.timestamp())
                job = Job(
                    name=periodic.job_name,
                    id=f"periodic:{periodic.job_name}:{periodic.spec}:{epoch}",
                    enqueued_at=epoch,
                    args=None,
                )
                pipeline.zadd(self._key("scheduled"), {job.to_wire(): epoch})
                moment = periodic.schedule.next_after(moment)
        pipeline.set(self._key("last_periodic_enqueue"), now)
        pipeline.execute()

    # -- dead pool reaper, mirroring dead_pool_reaper.go --

    def _reap_loop(self) -> None:
        if self._stop.wait(self.dead_time):
            return
        while True:
            try:
                self._reap()
            except Exception:
                logger.exception("reap failed")
            if self._stop.wait(self.reap_period + random.randint(0, REAP_JITTER_SECONDS)):
                return

    def _reap(self) -> None:
        for dead_pool_id, job_names in self._find_dead_pools().items():
            lock_job_names = job_names
            if job_names:
                self._requeue_in_progress(dead_pool_id, job_names)
                self.redis.delete(self._key(f"worker_pools:{dead_pool_id}"))
            else:
                lock_job_names = list(self.handlers)
            self.redis.srem(self._key("worker_pools"), dead_pool_id)
            self._clean_stale_lock_info(dead_pool_id, lock_job_names)
            logger.info("reaped dead pool", extra={"dead_pool_id": dead_pool_id, "jobs": job_names})

    def _find_dead_pools(self) -> dict[str, list[str]]:
        dead = {}
        for member in self.redis.smembers(self._key("worker_pools")):
            pool_id = member.decode()
            if pool_id == self.pool_id:
                continue
            heartbeat_key = self._key(f"worker_pools:{pool_id}")
            heartbeat_at = self.redis.hget(heartbeat_key, "heartbeat_at")
            if heartbeat_at is None:
                dead[pool_id] = []
                continue
            if int(heartbeat_at) + self.dead_time > time.time():
                continue
            job_names = self.redis.hget(heartbeat_key, "job_names")
            if job_names is None:
                continue
            dead[pool_id] = job_names.decode().split(",")
        return dead

    def _requeue_in_progress(self, dead_pool_id: str, job_names: list[str]) -> None:
        keys = []
        for name in job_names:
            jobs = self._jobs_key(name)
            keys += [f"{jobs}:{dead_pool_id}:inprogress", jobs, f"{jobs}:lock", f"{jobs}:lock_info"]
        while self.redis.eval(REENQUEUE_JOB, len(keys), *keys, dead_pool_id) is not None:
            pass

    def _clean_stale_lock_info(self, dead_pool_id: str, job_names: list[str]) -> None:
        if not job_names:
            return
        keys = []
        for name in job_names:
            jobs = self._jobs_key(name)
            keys += [f"{jobs}:lock", f"{jobs}:lock_info"]
        self.redis.eval(REAP_STALE_LOCKS, len(keys), *keys, dead_pool_id)

    # -- heartbeat, mirroring heartbeater.go --

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_period):
            try:
                self._heartbeat()
            except Exception:
                # A dying heartbeat thread would get a live pool reaped and
                # its in-flight jobs run twice; Go logs and keeps beating.
                logger.exception("heartbeat failed", extra={"pool_id": self.pool_id})

    def _heartbeat(self) -> None:
        pipeline = self.redis.pipeline(transaction=False)
        pipeline.sadd(self._key("worker_pools"), self.pool_id)
        pipeline.hset(
            self._key(f"worker_pools:{self.pool_id}"),
            mapping={
                "heartbeat_at": int(time.time()),
                "started_at": self.started_at or int(time.time()),
                "job_names": ",".join(sorted(self.handlers)),
                "concurrency": self.concurrency,
                "worker_ids": ",".join(sorted(self.worker_ids)),
                "host": socket.gethostname(),
                "pid": os.getpid(),
            },
        )
        pipeline.execute()

    def _remove_heartbeat(self) -> None:
        pipeline = self.redis.pipeline(transaction=False)
        pipeline.srem(self._key("worker_pools"), self.pool_id)
        pipeline.delete(self._key(f"worker_pools:{self.pool_id}"))
        pipeline.execute()
