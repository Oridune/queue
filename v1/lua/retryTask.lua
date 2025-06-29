-- Lua script to update task error
--
-- KEYS:
-- KEYS[1] - namespace
-- KEYS[2] - topic
-- ARGV:
-- ARGV[1 ... n] - optional list of taskIds
--
-- Declare variables
local namespace = KEYS[1]
local topic = KEYS[2]

local failedKey = namespace .. ':' .. topic .. ':' .. 'failed'
local waitingKey = namespace .. ':' .. topic .. ':' .. 'waiting'

for i = 1, #ARGV do
    local taskId = ARGV[i]
    local priority = redis.call('ZSCORE', failedKey, taskId);

    if priority then
        redis.call('ZREM', failedKey, taskId)
        redis.call('ZADD', waitingKey, priority, taskId)
    end
end

-- Return success
return 1
