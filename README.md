# pygowork

[![tests](https://github.com/HaulCo/pygowork/actions/workflows/tests.yml/badge.svg)](https://github.com/HaulCo/pygowork/actions/workflows/tests.yml)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![python](https://img.shields.io/badge/python-3.12%2B-blue?logo=python&logoColor=white)](https://github.com/HaulCo/pygowork/blob/main/pyproject.toml)
[![gocraft/work](https://img.shields.io/badge/gocraft%2Fwork-v0.5.1_compatible-8b5cf6)](https://github.com/gocraft/work)
[![redis](https://img.shields.io/badge/backed_by-Redis-d82c20?logo=redis&logoColor=white)](https://redis.io)

[gocraft/work](https://github.com/gocraft/work) compatible job queue client
for Python. Enqueue and process Redis-backed jobs interoperably with Go
worker pools: same queues, same wire format, same semantics, any mix of Go
and Python workers.

If you run a Go fleet on gocraft/work and want a Python service on the same
queues, this is the whole story: no broker to add, no bridge service, no
HTTP hop. The Python pool heartbeats, retries, dead-letters, and reaps
exactly like its Go neighbors, and they cover each other.

## install

Not on PyPI yet. Install from source:

```sh
pip install git+https://github.com/HaulCo/pygowork
```

## produce

```python
from redis import Redis
from pygowork import Producer

producer = Producer(Redis(), namespace="my_app")
producer.enqueue("send_email", {"address": "jo@example.com", "subject": "hi"})
producer.enqueue_in("send_reminder", 3600, {"address": "jo@example.com"})
producer.enqueue_unique("rebuild_index", {"segment": 4})  # None if already queued
```

A Go pool on the `my_app` namespace picks these up like any other job.

## consume

```python
from redis import Redis
from pygowork import Job, JobOptions, PeriodicJob, WorkerPool

def send_email(job: Job) -> None:
    job.checkin("connecting to smtp")  # visible in worker observations
    address = job.arg("address", str)  # ArgumentError on missing or mistyped,
    subject = job.arg("subject", str)  # naming the job, key, and both types
    deliver(address, subject)
    # raising retries the job with gocraft's backoff curve; after max_fails
    # failures it lands in the dead set, where job.arg's message reads
    # straight off last_err

def hourly_rollup(job: Job) -> None:
    rollup()

pool = WorkerPool(
    Redis(),
    namespace="my_app",
    handlers={"send_email": send_email, "hourly_rollup": hourly_rollup},
    concurrency=5,
    priorities={"send_email": 5, "hourly_rollup": 1},
    options={"send_email": JobOptions(max_fails=2, backoff=lambda job: 30)},
    periodic=[PeriodicJob(spec="0 0 * * * *", job_name="hourly_rollup")],
)
pool.run(handle_signals=True)  # blocking; SIGTERM/SIGINT finish in-flight
                               # jobs and exit cleanly
```

Jobs produced by Go pools land in these handlers too, and jobs produced
here land in Go handlers.

## administer

```python
from redis import Redis
from pygowork import Client

client = Client(Redis(), namespace="my_app")
for queue in client.queues():
    print(queue.job_name, queue.count, queue.latency)

dead_rows, total = client.dead_jobs(page=1)
client.retry_all_dead_jobs()
```

## how compatibility works

gocraft/work's multi-key atomic operations live in Redis Lua scripts. This
library carries those scripts verbatim and invokes them with the same keys,
so the semantics are not imitated, they are identical: Redis executes the
same logic regardless of which language sent it. Everything around the Lua
(wire format, key layout, heartbeat fields, backoff curve, periodic job
identifiers) is matched against the gocraft/work v0.5.1 sources.

Carried surface:

- producers: `enqueue`, `enqueue_in`, `enqueue_unique`, `enqueue_unique_in`
- worker pool: fetch, completion, retry with gocraft's backoff, dead-letter,
  unique-signature cleanup, heartbeats, the requeuer, the dead-pool reaper,
  periodic enqueues (robfig six-field specs, the `@descriptors`, and
  `@every` Go durations), worker observations with `checkin`, priority
  weighting, pause keys, `drain`, and per-job options (`JobOptions`:
  priority, max fails, skip dead, custom backoff, max concurrency, with the
  `max_concurrency` key written on startup exactly as Go does)
- admin client: the full `client.go` surface, returned as typed dataclasses

Not carried: the web UI, middleware (see below), and typed contexts.
Go's `ArgString` family of argument
coercers is replaced rather than ported: they exist to work around Go
decoding every JSON number as `float64`, which Python does not do, so
pygowork keeps just their good idea as `job.arg(key, type)` with a clear
error on a missing or mistyped argument (`job.args` stays available for
direct reads). `job.arg` also absorbs one wire reality: Go marshals
`float64(1.0)` as `1`, which arrives as an int, so asking for a `float`
accepts whole-number ints.

Documented divergences: worker observations are written per job rather than
batched on upstream's one-second ticker (same keys, same fields), and a
pool fails fast on an unreachable Redis where Go's lazy connection pool
idles.

### middleware: use closures

gocraft's middleware chain is process-local ergonomics, not protocol:
nothing about it touches Redis, so there is no compatibility to carry. It
exists in Go because handlers are rigid typed functions and the context
struct needs a mechanism to fill it per job. In Python the same thing is a
closure around the handler:

```python
def with_transaction(handler):
    """One database transaction per job: commit on success, roll back on
    failure. The classic per-request middleware, per job instead."""
    def wrapped(job):
        with session_factory.begin() as session:
            job.session = session  # handlers read it off the job, like a Go context
            handler(job)
    return wrapped

def with_error_tracking(handler):
    def wrapped(job):
        try:
            handler(job)
        except Exception as job_error:
            sentry_sdk.capture_exception(job_error)
            raise  # re-raise: the job must still fail into gocraft's retry flow
    return wrapped

handlers = {"send_email": send_email, "sync_invoice": sync_invoice}
handlers = {
    name: with_error_tracking(with_transaction(handler))
    for name, handler in handlers.items()
}
pool = WorkerPool(Redis(), namespace="my_app", handlers=handlers)
```

The semantics line up with Go middleware exactly: wrappers compose in
order with the outermost running first, and an exception that escapes a
wrapper is a handler failure, so the job retries with gocraft's backoff
just as it would for a Go middleware returning an error. That is why
`with_error_tracking` re-raises after reporting.

### one namespace, one job registry

Inherited from gocraft: every pool's requeuer moves due retry and scheduled
jobs it recognizes and dead-letters ones it does not. On a namespace where
pools register different job subsets, each pool's requeuer can dead-letter
the other's due jobs. This is true of two Go pools just as much as a mixed
fleet. Either register every job name in the namespace on every pool, or
give heterogeneous pools separate namespaces.

## testing

```sh
make test
```

The suite mirrors gocraft/work's own tests and runs against a real local
Redis on the default port (nothing is mocked). It works in the
`pygowork_test` namespace and cleans that keyspace before and after every
test. Verbose output lands in `.repo/test-logs/latest.log`.

## license

MIT. Carries portions of gocraft/work, also MIT, copyright Jonathan Novak;
that notice lives at `src/pygowork/lua/LICENSE-gocraft-work` and ships
inside the package.
