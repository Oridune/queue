-- Lua script to fetch tasks and move them to the processing set atomically
--
-- KEYS:
-- KEYS[1] - Namespace
-- KEYS[2] - Count
-- KEYS[3] - Sort
--
-- Declare variables
local namespace = KEYS[1];
local taskCount = tonumber(KEYS[2])
local sort = tonumber(KEYS[3])

local delayedPrefix = namespace .. ":" .. "delayed"
local waitingPrefix = namespace .. ":" .. "waiting"
local processingPrefix = namespace .. ":" .. "processing"

local time = redis.call("TIME")
local timestamp = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

-- Fetch delayed tasks
local delayedIds = {}

if taskCount > 0 then
    delayedIds = redis.call('ZRANGEBYSCORE', delayedPrefix, '-inf', timestamp, 'LIMIT', 0, taskCount)
else
    delayedIds = redis.call('ZRANGEBYSCORE', delayedPrefix, '-inf', timestamp)
end

-- Fetch waiting tasks if needed
local waitingIds = {}

if taskCount > 0 then
    if #delayedIds < taskCount then
        local remaining = taskCount - #delayedIds

        if sort > 0 then
            waitingIds = redis.call('ZRANGEBYSCORE', waitingPrefix, '-inf', '+inf', 'LIMIT', 0, remaining)
        else
            waitingIds = redis.call('ZREVRANGEBYSCORE', waitingPrefix, '+inf', '-inf', 'LIMIT', 0, remaining)
        end
    end
else
    if sort > 0 then
        waitingIds = redis.call('ZRANGEBYSCORE', waitingPrefix, '-inf', '+inf')
    else
        waitingIds = redis.call('ZREVRANGEBYSCORE', waitingPrefix, '+inf', '-inf')
    end
end

-- Move tasks to the processing set
-- Move delayed tasks
if #delayedIds > 0 then
    local zaddArgs = {}

    for _, id in ipairs(delayedIds) do
        table.insert(zaddArgs, timestamp + 10000)
        table.insert(zaddArgs, id)
    end

    redis.call('ZADD', processingPrefix, unpack(zaddArgs))
    redis.call('ZREM', delayedPrefix, unpack(delayedIds))
end

-- Move waiting tasks
if #waitingIds > 0 then
    local zaddArgs = {}

    for _, id in ipairs(waitingIds) do
        table.insert(zaddArgs, timestamp + 10000)
        table.insert(zaddArgs, id)
    end

    redis.call('ZADD', processingPrefix, unpack(zaddArgs))
    redis.call('ZREM', waitingPrefix, unpack(waitingIds))
end

-- Return the list of moved task IDs
local movedIds = {}

for _, id in ipairs(delayedIds) do
    table.insert(movedIds, id)
end

for _, id in ipairs(waitingIds) do
    table.insert(movedIds, id)
end

return movedIds
