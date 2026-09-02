"""Real-Redis test session, mirroring upstream's harness: every test starts
and ends with a clean pygowork_test keyspace (their cleanKeyspace). Nothing
is mocked; a local Redis must be reachable on the default port."""

import pytest
from redis import Redis

from helpers import NAMESPACE


def clean_keyspace(redis_client: Redis) -> None:
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(
            cursor=cursor, match=f"{NAMESPACE}:*", count=500
        )
        if keys:
            redis_client.delete(*keys)
        if cursor == 0:
            break


@pytest.fixture(scope="session")
def redis_client() -> Redis:
    client = Redis()
    client.ping()
    return client


@pytest.fixture(autouse=True)
def clean(redis_client: Redis):
    clean_keyspace(redis_client)
    yield
    clean_keyspace(redis_client)
