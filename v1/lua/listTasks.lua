-- Lua script to list tasks with optional field fetching
--
-- KEYS:
-- KEYS[1] - sorted_set_key (e.g., "topic:status")
-- KEYS[2] - dataKeyPrefix (e.g., "topic:data")
-- ARGV:
-- ARGV[1] - sort direction (1 for ZRANGEBYSCORE, -1 for ZREVRANGEBYSCORE)
-- ARGV[2] - offset (number of elements to skip)
-- ARGV[3] - limit (maximum number of elements to retrieve)
-- ARGV[4 ... n] - optional list of fields to fetch from each hash
--
-- Declare variables
local sorted_set_key = KEYS[1]
local dataKeyPrefix = KEYS[2]
local sort_dir = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

-- Extract fields list if provided
local fields = {}
for i = 4, #ARGV do
    table.insert(fields, ARGV[i])
end

-- Validate sort_dir
if sort_dir ~= 1 and sort_dir ~= -1 then
    error("Invalid sort direction. Use 1 for ascending or -1 for descending.")
end

-- Retrieve task IDs based on sort direction
local ids
if sort_dir > 0 then
    ids = redis.call('ZRANGEBYSCORE', sorted_set_key, '-inf', '+inf', 'LIMIT', offset, limit)
else
    ids = redis.call('ZREVRANGEBYSCORE', sorted_set_key, '+inf', '-inf', 'LIMIT', offset, limit)
end

-- Initialize the result table
local tasks = {}

-- Iterate over each ID and fetch task details
for i, id in ipairs(ids) do
    local dataKey = dataKeyPrefix .. ':' .. id
    if #fields > 0 then
        -- Fetch specified fields using HMGET
        local task = redis.call('HMGET', dataKey, unpack(fields))
        table.insert(tasks, task)
    else
        -- Fetch all fields using HGETALL
        local task = redis.call('HGETALL', dataKey)
        table.insert(tasks, task)
    end
end

-- Return the list of tasks
return tasks
