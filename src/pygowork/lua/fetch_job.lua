-- Carried verbatim from gocraft/work v0.5.1 (redis.go, redisLuaFetchJob),
-- used under the MIT License, copyright (c) 2016 Jonathan Novak.
-- See LICENSE-gocraft-work in this directory.
-- Only change from upstream: Go templates the keys-per-job-type stride via
-- fmt.Sprintf; the constant (fetchKeysPerJobType = 6, worker.go) is inlined.
--
-- KEYS layout, six per registered job type, in this order:
--   jobs, in-progress (per pool), paused, lock, lock_info, max_concurrency
-- ARGV[1] = worker pool id
-- Returns {job json, source queue, in-progress queue} or nil.
local function acquireLock(lockKey, lockInfoKey, workerPoolID)
  redis.call('incr', lockKey)
  redis.call('hincrby', lockInfoKey, workerPoolID, 1)
end

local function haveJobs(jobQueue)
  return redis.call('llen', jobQueue) > 0
end

local function isPaused(pauseKey)
  return redis.call('get', pauseKey)
end

local function canRun(lockKey, maxConcurrency)
  local activeJobs = tonumber(redis.call('get', lockKey))
  if (not maxConcurrency or maxConcurrency == 0) or (not activeJobs or activeJobs < maxConcurrency) then
    -- default case: maxConcurrency not defined or set to 0 means no cap on concurrent jobs OR
    -- maxConcurrency set, but lock does not yet exist OR
    -- maxConcurrency set, lock is set, but not yet at max concurrency
    return true
  else
    -- we are at max capacity for running jobs
    return false
  end
end

local res, jobQueue, inProgQueue, pauseKey, lockKey, maxConcurrency, workerPoolID, concurrencyKey, lockInfoKey
local keylen = #KEYS
workerPoolID = ARGV[1]

for i=1,keylen,6 do
  jobQueue = KEYS[i]
  inProgQueue = KEYS[i+1]
  pauseKey = KEYS[i+2]
  lockKey = KEYS[i+3]
  lockInfoKey = KEYS[i+4]
  concurrencyKey = KEYS[i+5]

  maxConcurrency = tonumber(redis.call('get', concurrencyKey))

  if haveJobs(jobQueue) and not isPaused(pauseKey) and canRun(lockKey, maxConcurrency) then
    acquireLock(lockKey, lockInfoKey, workerPoolID)
    res = redis.call('rpoplpush', jobQueue, inProgQueue)
    return {res, jobQueue, inProgQueue}
  end
end
return nil
