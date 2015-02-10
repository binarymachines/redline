--[[

msg_stats script for Redline message queue

store queuing statistics for a given message ID

Dexter Taylor, author
binarymachineshop@gmail.com

--]]


local print = print
local redis = redis

module(msg_stats, package.seeall)


local msg_stats = {}

function msg_stats.loadData(stats_table_key, message_id)
         print('Hello world')
         print('loading stats from ' .. stats_table_key .. ' for message ID ' .. message_id)
         msg_stats = redis.call('HGET', stats_table_key, message_id)         
         return msg_stats
end


return msg_stats.loadData



