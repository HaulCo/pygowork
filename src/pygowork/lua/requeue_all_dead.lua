-- Carried verbatim from gocraft/work v0.5.1 (redis.go, redisLuaRequeueAllDeadCmd),
-- used under the MIT License, copyright (c) 2016 Jonathan Novak.
-- See LICENSE-gocraft-work in this directory.
-- KEYS[1] = dead zset, KEYS[2...] = known job queues.
-- ARGV[1] = jobs prefix, ARGV[2] = now, ARGV[3] = max jobs to requeue.
local jobs, i, j, queue, found, requeuedCount
jobs = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, ARGV[3])
local jobCount = #jobs
requeuedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  redis.call('zrem', KEYS[1], jobs[i])
  queue = ARGV[1] .. j['name']
  found = false
  for _,v in pairs(KEYS) do
    if v == queue then
      j['t'] = tonumber(ARGV[2])
      j['fails'] = nil
      j['failed_at'] = nil
      j['err'] = nil
      redis.call('lpush', queue, cjson.encode(j))
      requeuedCount = requeuedCount + 1
      found = true
      break
    end
  end
  if not found then
    j['err'] = 'unknown job when requeueing'
    j['failed_at'] = tonumber(ARGV[2])
    redis.call('zadd', KEYS[1], ARGV[2] + 5, cjson.encode(j))
  end
end
return requeuedCount
