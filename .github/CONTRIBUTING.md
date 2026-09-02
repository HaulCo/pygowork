# Contributing to pygowork

Thanks for helping out. One idea governs everything here:

**Compatibility is the product.** gocraft/work v0.5.1 is the specification.
When a question comes up about how something should behave, the answer is
whatever the Go implementation does, including its quirks. A faithful quirk
beats a sensible improvement.

What that means in practice:

- Behavior changes must cite the upstream Go source they match
  (file and function, e.g. `client.go RetryAllDeadJobs`).
- Files under `src/pygowork/lua/` are verbatim carries from upstream and
  are never edited. If upstream's Lua changes in a version we target, the
  new script is carried whole, with the attribution header kept.
- Where pygowork deliberately diverges (see the README), the divergence is
  documented in the module docstring and the README. Undocumented
  divergences are bugs.
- Features that do not exist in gocraft/work need an issue first. The bar
  for growing the surface beyond upstream is high.

## development setup

You need Python 3.12+ and a local Redis on the default port.

```sh
git clone https://github.com/HaulCo/pygowork
cd pygowork
python3 -m venv venv && source venv/bin/activate
pip install -e . pytest
make test
```

## tests

The suite mirrors upstream's test files and runs against real Redis in the
`pygowork_test` namespace, cleaning that keyspace before and after every
test. Nothing is mocked, including the clock: tests that need determinism
get it by construction (anchored timing, computed expectations) rather than
by patching.

- New behavior gets a test that mirrors the upstream test where one exists.
- Statistical assertions must be effectively failure-proof: size the sample
  so a correct implementation fails with negligible probability (see
  `tests/test_priorities.py` for the pattern).
- Verbose output, including per-test logs, lands in
  `.repo/test-logs/latest.log`.

## style

- Descriptive names everywhere; no single-letter variables, including loop
  indices and lambdas.
- Real classes over dicts on the public surface (see `job.py` and the
  client's result dataclasses).
- Builtin generics and unions (`dict[str, Any]`, `str | None`); import from
  `typing` only what has no builtin home.
- Comments explain what the code cannot say itself, most often which
  upstream behavior a line is matching.

## pull requests

Before opening one:

- `make test` passes against your local Redis.
- Anything behavioral links the upstream source it matches.
- The README's carried-surface and divergence lists still tell the truth.
