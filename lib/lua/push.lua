-- KEYS[1] - the messages queue key
-- KEYS[2] - the messages id key
-- ARGV[1] - the message to push

-- increment the message counter
local id = redis.call('incr', KEYS[2])

-- push the id and the message
redis.call('rpush', KEYS[1], id)
redis.call('rpush', KEYS[1], ARGV[1])

-- return the id
return tonumber(id)
