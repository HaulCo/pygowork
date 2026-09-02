"""Loads the Redis Lua scripts this package carries from gocraft/work.
The scripts live as .lua files beside this module; each carries its own
attribution header. LICENSE-gocraft-work in the same directory carries the notice.
"""

from pathlib import Path

LUA_DIR = Path(__file__).parent / "lua"

FETCH_JOB = (LUA_DIR / "fetch_job.lua").read_text()
REENQUEUE_JOB = (LUA_DIR / "reenqueue_job.lua").read_text()
REAP_STALE_LOCKS = (LUA_DIR / "reap_stale_locks.lua").read_text()
ZREM_LPUSH = (LUA_DIR / "zrem_lpush.lua").read_text()
ENQUEUE_UNIQUE = (LUA_DIR / "enqueue_unique.lua").read_text()
ENQUEUE_UNIQUE_IN = (LUA_DIR / "enqueue_unique_in.lua").read_text()
DELETE_SINGLE = (LUA_DIR / "delete_single.lua").read_text()
REQUEUE_SINGLE_DEAD = (LUA_DIR / "requeue_single_dead.lua").read_text()
REQUEUE_ALL_DEAD = (LUA_DIR / "requeue_all_dead.lua").read_text()
