-- Lua script to update task error
--
-- KEYS:
-- KEYS[1] - namespace
-- KEYS[2] - topic
-- KEYS[3] - taskId
-- KEYS[4] - uuid
-- ARGV:
-- ARGV[1] - message (error message string)
-- ARGV[2] - stack (error stack string)
-- ARGV[3] - timestamp (integer, milliseconds since epoch)
-- ARGV[4] - moveToFailed (0 - 1)
-- ARGV[5] - priority
--
-- Declare variables
local namespace = KEYS[1]
local topic = KEYS[2]
local taskId = KEYS[3]
local uuid = KEYS[4]

local message = ARGV[1]
local stack = ARGV[2]
local timestamp = tonumber(ARGV[3])
local moveToFailed = tonumber(ARGV[4])
local priority = tonumber(ARGV[5]) or 0

local dataKey = namespace .. ':' .. topic .. ':' .. 'data' .. ':' .. taskId
local processingKey = namespace .. ':' .. topic .. ':' .. 'processing'
local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
local errorListKey = dataKey .. ':' .. 'error'
local errorListItemKey = errorListKey .. ':' .. uuid

-- Push the UUID to the errorListKey
redis.call('RPUSH', errorListKey, uuid)

-- Set multiple fields in errorDetailsKey
redis.call('HSET', errorListItemKey, 'message', message, 'stack', stack, 'timestamp', timestamp)

if moveToFailed > 0 then
    redis.call('ZREM', processingKey, taskId)
    redis.call('ZADD', failedKey, priority, taskId)
end

-- Return success
return 1
