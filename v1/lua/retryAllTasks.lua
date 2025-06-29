-- Lua script to update task error
--
-- KEYS:
-- KEYS[1] - namespace
-- KEYS[2] - topic
--
-- Declare variables
local namespace = KEYS[1]
local topic = KEYS[2]

local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
local waitingKey = namespace .. ':' .. topic .. ':' .. 'waiting'

local tasks = redis.call('ZRANGE', failedKey, 0, -1, 'WITHSCORES')

for i = 1, #tasks, 2 do
    local taskId = tasks[i]
    local priority = tasks[i + 1]

    redis.call('ZREM', failedKey, taskId)
    redis.call('ZADD', waitingKey, priority, taskId)
end

-- Return success
return 1
