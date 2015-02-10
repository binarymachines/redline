--[[

queue_msg script for Redline message queue

places the passed message ID on the pending list,
stores the message payload in the messages table under the ID,
and update the stats table

Dexter Taylor, author
binarymachineshop@gmail.com


          self.set('enqueue_time', self.currentDatestamp())
          self.set('last_dequeue_time', None)
          self.set('dequeue_count',  0)
          self.set('last_requeue_time', None)
          self.set('last_requeue_count', 0)

--]]



-- KEYS
local values_table = 1
local pending_list = 2
local stats_table = 3

-- ARGS
local msg_key = 1
local payload = 2


package.path = package.path .. ";/opt/local/redis/scripts/?.lua"

redis.call('HSET', KEYS[values_table], ARGV[msg_key], ARGV[payload])
local result = redis.call('RPUSH', KEYS[pending_list], ARGV[msg_key])
--local msg_stats = redis.call('HGET', KEYS[stats_table], ARGV[msg_key])

--print(msg_stats)


   local msg_stats = {}
   msg_stats['enqueue_time'] = os.time()
   msg_stats['last_dequeue_time'] = nil
   msg_stats['dequeue_count'] = 0
   msg_stats['last_requeue_time'] = nil
   msg_stats['requeue_count'] = 0
   

redis.call('HMSET', KEYS[stats_table], ARGV[msg_key], {foo = 'bar', billy = 'bats'})


return result
