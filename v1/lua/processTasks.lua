-- Lua script to fetch tasks and move them to the processing set atomically
--
-- KEYS:
-- KEYS[1] - Namespace
-- KEYS[2] - Count
-- KEYS[3] - Sort
-- KEYS[4] - Timestamp
--
-- Declare variables
local namespace = KEYS[1];
local taskCount = tonumber(KEYS[2])
local sort = tonumber(KEYS[3])
local timestamp = tonumber(KEYS[4])

local delayedPrefix = namespace .. ":" .. "delayed"
local waitingPrefix = namespace .. ":" .. "waiting"
local processingPrefix = namespace .. ":" .. "processing"

-- Fetch delayed tasks (with scores)
local delayedIds = {}

if taskCount > 0 then
    delayedIds = redis.call('ZRANGEBYSCORE', delayedPrefix, '-inf', timestamp, 'WITHSCORES', 'LIMIT', 0, taskCount)
else
    delayedIds = redis.call('ZRANGEBYSCORE', delayedPrefix, '-inf', timestamp, 'WITHSCORES')
end

-- Fetch waiting tasks if needed (with scores)
local waitingIds = {}

if taskCount > 0 then
    -- only fetch waiting if we still need more
    local fetchedCount = #delayedIds / 2 -- each item is (id, score)

    if fetchedCount < taskCount then
        local remaining = taskCount - fetchedCount

        if sort > 0 then
            waitingIds = redis.call('ZRANGEBYSCORE', waitingPrefix, '-inf', '+inf', 'WITHSCORES', 'LIMIT', 0, remaining)
        else
            waitingIds = redis.call('ZREVRANGEBYSCORE', waitingPrefix, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0,
                remaining)
        end
    end
else
    if sort > 0 then
        waitingIds = redis.call('ZRANGEBYSCORE', waitingPrefix, '-inf', '+inf', 'WITHSCORES')
    else
        waitingIds = redis.call('ZREVRANGEBYSCORE', waitingPrefix, '+inf', '-inf', 'WITHSCORES')
    end
end

-- Move delayed tasks to the processing set using their original scores
if #delayedIds > 0 then
    local zaddArgs = {}
    local delayedMembers = {}

    -- delayedIds = { id1, score1, id2, score2, ... }
    for i = 1, #delayedIds, 2 do
        local id = delayedIds[i]
        local score = tonumber(delayedIds[i + 1])

        table.insert(zaddArgs, score)
        table.insert(zaddArgs, id)

        table.insert(delayedMembers, id)
    end

    redis.call('ZADD', processingPrefix, unpack(zaddArgs))
    redis.call('ZREM', delayedPrefix, unpack(delayedMembers))
end

-- Move waiting tasks to the processing set using their original scores
if #waitingIds > 0 then
    local zaddArgs = {}
    local waitingMembers = {}

    -- waitingIds = { id1, score1, id2, score2, ... }
    for i = 1, #waitingIds, 2 do
        local id = waitingIds[i]
        local score = tonumber(waitingIds[i + 1])

        table.insert(zaddArgs, score)
        table.insert(zaddArgs, id)

        table.insert(waitingMembers, id)
    end

    redis.call('ZADD', processingPrefix, unpack(zaddArgs))
    redis.call('ZREM', waitingPrefix, unpack(waitingMembers))
end

-- Return the list of moved task IDs (only IDs, no scores)
local movedIds = {}

for i = 1, #delayedIds, 2 do
    table.insert(movedIds, delayedIds[i])
end

for i = 1, #waitingIds, 2 do
    table.insert(movedIds, waitingIds[i])
end

return movedIds
