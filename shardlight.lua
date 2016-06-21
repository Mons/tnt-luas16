local obj = require('obj')
local log = require('log')
local fiber = require('fiber')
local digest = require('digest')

local quickpool = require('quickpool')


local M = obj.class({}, 'shardlight')


function M:_init(cfg)
	log.info('Sharding initialization started...')
	cfg.grouped = true
	self.pool = quickpool(cfg)
	self.raft_enabled = cfg.raft_enabled or false
	
	self.shards = self:setup_shards()
	self.shards_n = #self.shards
end


function M:setup_shards()
	local shards = {}
	
	for group_id, group in ipairs(self.pool.peer_groups) do
		shards[group_id] = {}
		for _, srv in ipairs(group) do
			table.insert(shards[group_id], srv)
		end
	end
	
	return shards
end


-- main shards search function (from https://github.com/tarantool/shard/blob/master/shard.lua)
function M:shard(key, include_dead)
    local num
    if type(key) == 'number' then
        num = key
    else
        num = digest.crc32(key)
    end
    local shard_id = 1 + digest.guava(num, self.shards_n)
    local shard = self.shards[shard_id]
    -- local res = {}
    -- local k = 1
    -- for i = 1, redundancy do
    --     local srv = shard[i]
    --     if pool:server_is_ok(srv) or include_dead then
    --         res[k] = srv
    --         k = k + 1
    --     end
    -- end
    -- return res
    return shard, shard_id
end

function M:connect()
	self.pool:connect()
end

function M:space_operation(shard_key, space, operation, ...)
	local shard, shard_id = self:shard(shard_key)
	
	local targets = {shard}
	if self.raft_enabled then
		targets = self.pool:get_leaders(shard_id)
	end
	
	local nodes = {}
	for _, srv in ipairs(targets) do
		nodes[srv.peer] = true
	end
	
	return self.pool:_operation_on_space(nodes, space, operation, ...)
end

function M:operation(shard_key, operation, ...)
	local shard, shard_id = self:shard(shard_key)
	
	local targets = {shard}
	if self.raft_enabled then
		targets = self.pool:get_leaders(shard_id)
	end
	
	local nodes = {}
	for _, srv in ipairs(targets) do
		nodes[srv.peer] = true
	end
	
	return self.pool:_operation(nodes, operation, ...)
end

function M:select(shard_key, space, ...)
	return self:space_operation(shard_key, space, 'select', ...)
end

function M:insert(shard_key, space, ...)
	return self:space_operation(shard_key, space, 'insert', ...)
end

function M:replace(shard_key, space, ...)
	return self:space_operation(shard_key, space, 'replace', ...)
end

function M:delete(shard_key, space, ...)
	return self:space_operation(shard_key, space, 'delete', ...)
end

function M:upsert(shard_key, space, ...)
	return self:space_operation(shard_key, space, 'upsert', ...)
end

function M:call(shard_key, ...)
	return self:operation(shard_key, 'call', ...)
end

function M:eval(shard_key, ...)
	return self:operation(shard_key, 'eval', ...)
end

return M

