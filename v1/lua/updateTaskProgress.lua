-- Lua script to update task progress
--
-- KEYS:
-- KEYS[1] - Namespace
-- KEYS[2] - uuid
-- KEYS[3] - percentage (integer or string representing integer)
-- KEYS[4] - log (string)
-- KEYS[5] - Timestamp
--
-- Declare variables
local namespace = KEYS[1]
local progressListKey = namespace .. ":" .. "progress"
local uuid = KEYS[2]
local percentage = tonumber(KEYS[3])
local log = KEYS[4]
local timestamp = tonumber(KEYS[5])

-- local time = redis.call("TIME")
-- local timestamp = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local progressListItemKey = progressListKey .. ':' .. uuid

-- Set the 'progress' field in namespace
redis.call('HSET', namespace, 'progress', percentage)

-- Push the UUID to the progressListKey
redis.call('RPUSH', progressListKey, uuid)

-- Set multiple fields in progressDetailsKey
redis.call('HSET', progressListItemKey, 'percentage', percentage, 'log', log, 'timestamp', timestamp)

-- Return success
return 1
