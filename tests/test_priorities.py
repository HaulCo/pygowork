"""Mirrors priority_sampler_test.go with upstream's statistical bounds:
priority 5 leads more than twice as often as priority 2, priority 2 beats
1.5x priority 1, priority 1 still gets its turn, and priority 1 lands last
more than half the time. Upstream draws 200 samples, which fails a correct
sampler roughly 8% of the time (the c5 > 2*c2 margin sits ~1.4 sigma from
its bound); the bounds are kept and the sample count raised to 2000, which
puts a false failure around 1e-5."""

from helpers import NAMESPACE
from pygowork import Job, JobOptions, WorkerPool


def noop(job: Job) -> None:
    return None


def test_priority_weighted_sampling(redis_client):
    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"five": noop, "two": noop, "one": noop},
        priorities={"five": 5, "two": 2, "one": 1},
        requeuer=False,
        reaper=False,
    )

    total = 2000
    first_counts = {"five": 0, "two": 0, "one": 0}
    one_last = 0
    for _ in range(total):
        ordered = pool._sampled_job_names()
        assert sorted(ordered) == ["five", "one", "two"]  # without replacement
        first_counts[ordered[0]] += 1
        if ordered[2] == "one":
            one_last += 1

    # make sure these numbers are roughly correct. note that probability is a thing.
    assert first_counts["five"] > 2 * first_counts["two"]
    assert first_counts["two"] > 1.5 * first_counts["one"]
    assert first_counts["one"] >= total / 13
    assert one_last > total * 0.50


def test_job_options_priority_resolution(redis_client):
    """JobOptions.priority overrides the priorities dict when set, and its
    None default never silently resets a priority configured there."""
    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"alpha": noop, "beta": noop},
        priorities={"alpha": 5, "beta": 2},
        options={"beta": JobOptions(priority=7)},
        requeuer=False,
        reaper=False,
    )
    assert pool.priorities == {"alpha": 5, "beta": 7}

    pool_with_unrelated_option = WorkerPool(
        redis_client,
        NAMESPACE,
        {"alpha": noop},
        priorities={"alpha": 5},
        options={"alpha": JobOptions(skip_dead=True)},
        requeuer=False,
        reaper=False,
    )
    assert pool_with_unrelated_option.priorities == {"alpha": 5}


def test_sampler_matches_exact_permutation_probabilities(redis_client):
    """The failure-proof version: weighted sampling without replacement gives
    every full ordering an exactly computable probability (draw a name with
    p = weight/total, remove it, repeat), so all six permutations are checked
    against 100,000 draws. The rarest permutation has p ~ 0.083 with a
    standard deviation of ~0.0009 at this sample size, so the +/-0.01
    tolerance sits ~11 sigma out: a false failure is on the order of 1e-30,
    while the real RNG path still runs unseeded."""
    priorities = {"five": 5, "two": 2, "one": 1}
    pool = WorkerPool(
        redis_client,
        NAMESPACE,
        {"five": noop, "two": noop, "one": noop},
        priorities=priorities,
        requeuer=False,
        reaper=False,
    )

    def permutation_probability(ordering: tuple[str, ...]) -> float:
        remaining = dict(priorities)
        probability = 1.0
        for name in ordering:
            probability *= remaining[name] / sum(remaining.values())
            del remaining[name]
        return probability

    total = 100_000
    observed = {}
    for _ in range(total):
        ordering = tuple(pool._sampled_job_names())
        observed[ordering] = observed.get(ordering, 0) + 1

    expected = {
        (first, second, third): permutation_probability((first, second, third))
        for first in priorities
        for second in priorities
        for third in priorities
        if len({first, second, third}) == 3
    }
    assert abs(sum(expected.values()) - 1.0) < 1e-12  # the model itself is coherent

    for ordering, probability in expected.items():
        frequency = observed.get(ordering, 0) / total
        assert (
            abs(frequency - probability) < 0.01
        ), f"{ordering}: observed {frequency:.4f}, expected {probability:.4f}"
