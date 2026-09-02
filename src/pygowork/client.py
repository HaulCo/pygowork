"""Admin client, mirroring gocraft/work's client.go: introspection of pools,
workers, queues, and the scheduled, retry, and dead zsets, plus dead-job
requeue and deletion. Results are dataclasses named after gocraft's types."""

import time
from dataclasses import dataclass

from redis import Redis

from pygowork.job import Job
from pygowork.lua import DELETE_SINGLE, REQUEUE_ALL_DEAD, REQUEUE_SINGLE_DEAD
from pygowork.producer import unique_job_key

PAGE_SIZE = 20
REQUEUE_ALL_BATCH = 1000


@dataclass
class WorkerPoolHeartbeat:
    worker_pool_id: str
    heartbeat_at: int
    started_at: int
    job_names: list[str]
    concurrency: int
    worker_ids: list[str]
    host: str
    pid: int


@dataclass
class WorkerObservation:
    worker_id: str
    is_busy: bool
    job_name: str | None = None
    job_id: str | None = None
    started_at: int | None = None
    args_json: str | None = None
    checkin: str | None = None
    checkin_at: int | None = None


@dataclass
class Queue:
    job_name: str
    count: int
    latency: int


@dataclass
class ScheduledJobRow:
    run_at: int
    job: Job


@dataclass
class RetryJobRow:
    retry_at: int
    job: Job


@dataclass
class DeadJobRow:
    died_at: int
    job: Job


class Client:
    def __init__(self, redis_client: Redis, namespace: str) -> None:
        self.redis = redis_client
        self.namespace = namespace

    def _key(self, suffix: str) -> str:
        return f"{self.namespace}:{suffix}"

    def _known_job_queues(self) -> list[str]:
        names = sorted(
            member.decode() for member in self.redis.smembers(self._key("known_jobs"))
        )
        return [self._key(f"jobs:{name}") for name in names]

    # -- introspection --

    def worker_pool_heartbeats(self) -> list[WorkerPoolHeartbeat]:
        heartbeats = []
        for member in sorted(self.redis.smembers(self._key("worker_pools"))):
            pool_id = member.decode()
            raw = self.redis.hgetall(self._key(f"worker_pools:{pool_id}"))
            fields = {key.decode(): value.decode() for key, value in raw.items()}
            heartbeats.append(
                WorkerPoolHeartbeat(
                    worker_pool_id=pool_id,
                    heartbeat_at=int(fields.get("heartbeat_at", 0)),
                    started_at=int(fields.get("started_at", 0)),
                    job_names=(
                        fields["job_names"].split(",")
                        if fields.get("job_names")
                        else []
                    ),
                    concurrency=int(fields.get("concurrency", 0)),
                    worker_ids=(
                        fields["worker_ids"].split(",")
                        if fields.get("worker_ids")
                        else []
                    ),
                    host=fields.get("host", ""),
                    pid=int(fields.get("pid", 0)),
                )
            )
        return heartbeats

    def worker_observations(self) -> list[WorkerObservation]:
        observations = []
        for beat in self.worker_pool_heartbeats():
            for worker_id in beat.worker_ids:
                raw = self.redis.hgetall(self._key(f"worker:{worker_id}"))
                fields = {key.decode(): value.decode() for key, value in raw.items()}
                observations.append(
                    WorkerObservation(
                        worker_id=worker_id,
                        is_busy=bool(fields),
                        job_name=fields.get("job_name"),
                        job_id=fields.get("job_id"),
                        started_at=(
                            int(fields["started_at"])
                            if fields.get("started_at")
                            else None
                        ),
                        args_json=fields.get("args") or None,
                        checkin=fields.get("checkin"),
                        checkin_at=(
                            int(fields["checkin_at"])
                            if fields.get("checkin_at")
                            else None
                        ),
                    )
                )
        return observations

    def queues(self) -> list[Queue]:
        now = int(time.time())
        queues = []
        for queue_key in self._known_job_queues():
            count = self.redis.llen(queue_key)
            latency = 0
            if count > 0:
                oldest = self.redis.lindex(queue_key, -1)
                if oldest:
                    latency = now - Job.from_wire(oldest).enqueued_at
            queues.append(
                Queue(
                    job_name=queue_key.rsplit(":", 1)[-1], count=count, latency=latency
                )
            )
        return queues

    # -- zset pages, 20 per page, 1-based, mirroring getZsetPage --

    def _zset_page(self, key: str, page: int) -> tuple[list[tuple[Job, int]], int]:
        if page < 1:
            page = 1
        start = (page - 1) * PAGE_SIZE
        rows = self.redis.zrange(key, start, start + PAGE_SIZE - 1, withscores=True)
        total = self.redis.zcard(key)
        return [(Job.from_wire(raw), int(score)) for raw, score in rows], total

    def scheduled_jobs(self, page: int = 1) -> tuple[list[ScheduledJobRow], int]:
        rows, total = self._zset_page(self._key("scheduled"), page)
        return [ScheduledJobRow(run_at=score, job=job) for job, score in rows], total

    def retry_jobs(self, page: int = 1) -> tuple[list[RetryJobRow], int]:
        rows, total = self._zset_page(self._key("retry"), page)
        return [RetryJobRow(retry_at=score, job=job) for job, score in rows], total

    def dead_jobs(self, page: int = 1) -> tuple[list[DeadJobRow], int]:
        rows, total = self._zset_page(self._key("dead"), page)
        return [DeadJobRow(died_at=score, job=job) for job, score in rows], total

    # -- mutations --

    def _delete_zset_job(self, key: str, score: int, job_id: str) -> tuple[bool, bytes]:
        deleted, job_bytes = self.redis.eval(DELETE_SINGLE, 1, key, score, job_id)
        return int(deleted) > 0, job_bytes

    def delete_dead_job(self, died_at: int, job_id: str) -> bool:
        ok, _ = self._delete_zset_job(self._key("dead"), died_at, job_id)
        return ok

    def retry_dead_job(self, died_at: int, job_id: str) -> int:
        keys = [self._key("dead")] + self._known_job_queues()
        return self.redis.eval(
            REQUEUE_SINGLE_DEAD,
            len(keys),
            *keys,
            self._key("jobs:"),
            int(time.time()),
            died_at,
            job_id,
        )

    def retry_all_dead_jobs(self) -> int:
        keys = [self._key("dead")] + self._known_job_queues()
        # now is fixed across batches, like upstream: jobs the Lua dead-letters
        # back at now+5 can then never come due inside this loop. Upstream also
        # stops only on a batch that moves nothing, not on a short batch --
        # dead-lettered unknowns make a batch short while work remains.
        now = int(time.time())
        requeued = 0
        for _ in range(REQUEUE_ALL_BATCH):
            moved = self.redis.eval(
                REQUEUE_ALL_DEAD,
                len(keys),
                *keys,
                self._key("jobs:"),
                now,
                REQUEUE_ALL_BATCH,
            )
            if moved == 0:
                break
            requeued += moved
        return requeued

    def delete_all_dead_jobs(self) -> None:
        self.redis.delete(self._key("dead"))

    def delete_scheduled_job(self, run_at: int, job_id: str) -> bool:
        ok, job_bytes = self._delete_zset_job(self._key("scheduled"), run_at, job_id)
        if job_bytes:
            job = Job.from_wire(job_bytes)
            if job.unique:
                self.redis.delete(unique_job_key(self.namespace, job.name, job.args))
        return ok

    def delete_retry_job(self, retry_at: int, job_id: str) -> bool:
        ok, _ = self._delete_zset_job(self._key("retry"), retry_at, job_id)
        return ok
