

local link_id = redis.call('INCR', KEYS[1])
return link_id
