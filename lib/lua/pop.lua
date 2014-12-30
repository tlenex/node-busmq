-- KEYS[1] - the messages queue
-- KEYS[2] - the 'waiting for ack' queue

-- pop a message
local id = redis.call('lpop',KEYS[1])
local message = redis.call('lpop',KEYS[1])
if KEYS[2] and id and message then
    -- also place the message and id in the 'waiting for ack' queue
    redis.call('rpush', KEYS[2], id)
    redis.call('rpush', KEYS[2], message)
end

-- return the id and the message
if id and message then
    return {tonumber(id), message }
else
    return nil
end

