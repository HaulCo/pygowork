-- Carried verbatim from gocraft/work v0.5.1 (redis.go, redisLuaEnqueueUnique),
-- used under the MIT License, copyright (c) 2016 Jonathan Novak.
-- See LICENSE-gocraft-work in this directory.
-- KEYS[1] = job queue, KEYS[2] = unique key. ARGV[1] = job json.
if redis.call('set', KEYS[2], '1', 'NX', 'EX', '86400') then
  redis.call('lpush', KEYS[1], ARGV[1])
  return 'ok'
end
return 'dup'
