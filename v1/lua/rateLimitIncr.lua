-- Lua script to increment until a limit atomically
--
-- KEYS:
-- KEYS[1] - Key
-- KEYS[2] - Limit
-- KEYS[3] - TTL
--
-- Declare variables
local key = KEYS[1]
local maxLimit = tonumber(KEYS[2])
local ttl = tonumber(KEYS[3])

local current = tonumber(redis.call("GET", key) or 0)

if current <= maxLimit then
  -- Allow increment
  redis.call("INCR", key)

  -- Set TTL if first time
  if current == 0 then
    redis.call("EXPIRE", key, ttl)
  end

  return current
end

-- Deny: limit exceeded
return maxLimit
