-- Carried verbatim from gocraft/work v0.5.1 (redis.go, redisLuaReapStaleLocks),
-- used under the MIT License, copyright (c) 2016 Jonathan Novak.
-- See LICENSE-gocraft-work in this directory.
-- KEYS: per job type: lock, lock_info. ARGV[1] = dead pool id.
local keylen = #KEYS
local lock, lockInfo, deadLockCount
local deadPoolID = ARGV[1]

for i=1,keylen,2 do
  lock = KEYS[i]
  lockInfo = KEYS[i+1]
  deadLockCount = tonumber(redis.call('hget', lockInfo, deadPoolID))

  if deadLockCount then
    redis.call('decrby', lock, deadLockCount)
    redis.call('hdel', lockInfo, deadPoolID)

    if tonumber(redis.call('get', lock)) < 0 then
      redis.call('set', lock, 0)
    end
  end
end
return nil
