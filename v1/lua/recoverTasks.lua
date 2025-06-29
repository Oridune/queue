-- Lua script to move expired (crashed) tasks from processing to failed
--
-- KEYS:
-- KEYS[1] - Namespace
-- KEYS[2] - Expired task threshold (ExpiredTaskMs)
--
-- Declare variables
local namespace = KEYS[1];
local thresholdMs = tonumber(KEYS[2])

local heartbeatPrefix = namespace .. ":" .. "heartbeat"
local processingPrefix = namespace .. ":" .. "processing"
local failedPrefix = namespace .. ":" .. "failed"
local dataPrefix = namespace .. ":" .. "data"

local time = redis.call("TIME")
local timestamp = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

-- Calculate the cutoff timestamp
local cutoffTimestamp = timestamp - thresholdMs

-- Fetch expired task IDs from the processing set
local failedIds = redis.call('ZRANGEBYSCORE', heartbeatPrefix, '-inf', cutoffTimestamp)

if #failedIds > 0 then
    -- Remove expired tasks from the heartbeat/processing set
    local unpackFailedIds = unpack(failedIds);

    redis.call('ZREM', heartbeatPrefix, unpackFailedIds)
    redis.call('ZREM', processingPrefix, unpackFailedIds)

    -- Prepare arguments for ZADD to add tasks to the failed set
    local zaddArgs = {}

    for _, id in ipairs(failedIds) do
        -- Construct the data key for each task
        local dataKey = dataPrefix .. ':' .. id

        -- Fetch the 'priority' field from the task's hash
        local priority = redis.call('HGET', dataKey, 'priority')
        
        if not priority then
            priority = '0' -- Default priority if not set
        end

        -- Convert priority to a number
        priority = tonumber(priority) or 0

        -- Append priority and task ID to the arguments
        table.insert(zaddArgs, priority)
        table.insert(zaddArgs, id)
    end

    -- Add tasks to the failed sorted set with their priorities as scores
    redis.call('ZADD', failedPrefix, unpack(zaddArgs))

    -- Return the list of moved task IDs
    return failedIds
else

    -- No expired tasks to process
    return {}
end
