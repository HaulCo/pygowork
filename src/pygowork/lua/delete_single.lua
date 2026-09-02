-- Carried verbatim from gocraft/work v0.5.1 (redis.go, redisLuaDeleteSingleCmd),
-- used under the MIT License, copyright (c) 2016 Jonathan Novak.
-- See LICENSE-gocraft-work in this directory.
-- KEYS[1] = dead|scheduled|retry zset. ARGV[1] = score, ARGV[2] = job id.
-- Returns {deleted count, job bytes}.
local jobs, i, j, deletedCount, jobBytes
jobs = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[1])
local jobCount = #jobs
jobBytes = ''
deletedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  if j['id'] == ARGV[2] then
    redis.call('zrem', KEYS[1], jobs[i])
    deletedCount = deletedCount + 1
    jobBytes = jobs[i]
  end
end
return {deletedCount, jobBytes}
