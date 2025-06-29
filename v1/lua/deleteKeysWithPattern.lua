-- Lua script to delete keys matching a pattern without returning the keys
--
-- KEYS:
-- KEYS[1]: The key pattern to match (e.g., "namespace:*")
-- ARGV:
-- ARGV[1]: The batch size (number of keys to delete per iteration)
--
-- Declare variables
local pattern = KEYS[1]
local batch_size = tonumber(ARGV[1]) or 10000 -- Default batch size

local cursor = "0"
local deleted_count = 0

repeat
    -- SCAN to iterate over keys matching the pattern
    local result = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", batch_size)
    cursor = result[1]
    local keys = result[2]

    if #keys > 0 then
        -- Delete keys in the current batch
        local deleted = redis.call("DEL", unpack(keys))
        deleted_count = deleted_count + (deleted or 0)
    end
until cursor == "0"

-- Return the total number of deleted keys
return deleted_count
