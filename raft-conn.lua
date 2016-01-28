local obj = require('obj')
local log = require('log')
local fiber = require('fiber')
local msgpack = require('msgpack')

local function bind(func, object)
    return function(...) return func(object, ...) end
end

local raft = require('raft')
local M = obj.class({}, 'raft-conn', raft)

local _local = {
	
}

function M:_init(cfg)
	self.name = cfg.name or 'default'
	self.raft_name = cfg.raft_name or 'default'
	self.debug = cfg.debug or false
	
	self.status_wait_timeout = cfg.status_wait_timeout or 2
	
	self.MODES = {
		EMBEDDED = 'embedded',
		STANDALONE = 'standalone',
	}
	
	if cfg.conn_pool then
		self.mode = self.MODES.EMBEDDED
		self._pool = cfg.conn_pool
		self._pool_nodes = cfg.servers
	else
		self.mode = self.MODES.STANDALONE
		
		local cp = require('quickpool')
		self._pool = cp {
			name = 'raft-'.. self.name..'-pool',
			login = cfg.login,
			password = cfg.password,
			servers = cfg.servers,
		}
		self._pool.on_connected = bind(self._pool_on_connected, self)
		self._pool.on_connected_one = bind(self._pool_on_connected_one, self)
		self._pool.on_disconnect_one = bind(self._pool_on_disconnect_one, self)
		self._pool.on_disconnect = bind(self._pool_on_disconnect, self)
	end
	
	self.pool = {}
	self:pool_init_functions()
	
	self.nodes_uuid_to_peer = {}
	
	self.srvs = {}
	self.leaders = {}
	self.leaders_total = 0
	self.leaders_counts = {}
	
	self.state_wait_fibers = {}
end

function M:start()
	if self.mode == self.MODES.STANDALONE then
		self._pool:connect()
	end
end

function M:get_info(uuid)
	local srv = self.srvs[uuid]
	while true do
		local node = self._pool:get_by_uuid(uuid)
		if node == nil then
			log.warn("[get_info] Lost node with uuid: %s", uuid)
			break
		end
		local r, e = pcall(node.conn.call, node.conn, self:_raft_func('info'))
		if r and e then
			local response = e[1][1]
			return response
		else
			log.warn("Error while info(). %s:%s", r, e)
			fiber.sleep(0.1)
		end
	end
end

function M:start_state_wait(uuid)
	self.state_wait_fibers[uuid] = fiber.create(function()
		fiber.self():name('fiber:state_wait')
		
		while true do
			local srv = self.srvs[uuid]
			local node = self._pool:get_by_uuid(uuid)
			if node == nil then
				log.warn("[state_wait] Lost node with uuid: %s", uuid)
				break
			end
			local r, e = pcall(node.conn.call, node.conn, self:_raft_func('state_wait'), self.status_wait_timeout)
			
			if r and e then
				-- log.info("Got state_wait for %s", uuid)
				local response = e[1][1]
				
				local prev_info = srv.info
				local new_info = response.info
				
				if prev_info ~= nil and prev_info.leader ~= nil then
					local prev_leader = prev_info.leader.uuid
					self.leaders[prev_leader][prev_info.uuid] = nil
					
					if self.leaders_counts[prev_leader] ~= nil then
						self.leaders_counts[prev_leader] = self.leaders_counts[prev_leader] - 1
					end
					
					if self.leaders_counts[prev_leader] == 0 then
						self.leaders[prev_leader] = nil
						self.leaders_total = self.leaders_total - 1
						self.leaders_counts[prev_leader] = nil
					end
				end
				
				if new_info ~= nil and new_info.leader ~= nil then
					local new_leader = new_info.leader.uuid
					if self.leaders[new_leader] == nil then
						self.leaders[new_leader] = {}
						self.leaders_total = self.leaders_total + 1
					end
					self.leaders[new_leader][new_info.uuid] = true
					
					if self.leaders_counts[new_leader] == nil then self.leaders_counts[new_leader] = 0 end
					self.leaders_counts[new_leader] = self.leaders_counts[new_leader] + 1
				end
				
				srv.info = new_info
			else
				log.warn("Error while state_wait. %s:%s", r, e)
				break
			end
		end
		
	end)
end

function M:stop_status_wait(uuid)
	if self.state_wait_fibers[uuid] ~= nil and self.state_wait_fibers[uuid]:status() == 'running' then
		self.state_wait_fibers[uuid]:cancel()
	end
	self.state_wait_fibers[uuid] = nil
end

function M:node_online(srv)
	if srv.info ~= nil and srv.info.leader ~= nil then
		local leader_uuid = srv.info.leader.uuid
		if self.leaders[leader_uuid] == nil then
			self.leaders[leader_uuid] = {}
			self.leaders_total = self.leaders_total + 1
		end
		self.leaders[leader_uuid][srv.info.uuid] = true
		
		if self.leaders_counts[leader_uuid] == nil then self.leaders_counts[leader_uuid] = 0 end
		self.leaders_counts[leader_uuid] = self.leaders_counts[leader_uuid] + 1
	end
end

function M:node_offline(srv)
	if srv == nil or srv.info == nil or srv.info.leader == nil then -- already cleaned up leader info
		return
	end
	local leader_uuid = srv.info.leader.uuid
	if leader_uuid == nil then
		return
	end
	
	if self.leaders[leader_uuid] ~= nil then
		self.leaders[leader_uuid][srv.info.uuid] = nil
		if self.leaders_counts[leader_uuid] ~= nil then
			self.leaders_counts[leader_uuid] = self.leaders_counts[leader_uuid] - 1
		end
		
		if self.leaders_counts[leader_uuid] == 0 then
			self.leaders[leader_uuid] = nil
			self.leaders_total = self.leaders_total - 1
			self.leaders_counts[leader_uuid] = nil
		end
	end
end


function M:_raft_func(func_name)
	return 'raft.' .. self.raft_name .. '.' .. func_name
end


function M:info()
	return {
		leaders = self.leaders,
		leaders_total = self.leaders_total,
		leaders_counts = self.leaders_counts,
		srvs = self.srvs,
	}
end

function M:get_leaders_uuids()
	local leaders = {}
	for leader_uuid,_ in pairs(self.leaders) do
		table.insert(leaders, leader_uuid)
	end
	return leaders
end


function M:run_on_leaders(method, ...)
	local peers = {}
	for uuid,_ in pairs(self.leaders) do
		local peer = self.nodes_uuid_to_peer[uuid]
		peers[peer] = true
	end
	return self._pool:_func(peers, method, ...)
end

function M:call_on_leaders(...)
	return self:run_on_leaders('call', ...)
end

function M:eval_on_leaders(...)
	return self:run_on_leaders('eval', ...)
end


---------------- pool functions ----------------

function M:pool_init_functions()
	if self.mode == self.MODES.EMBEDDED then
		local nodes = {}
		for _,n in ipairs(self._pool_nodes) do
			nodes[n] = true
		end
		self._pool_nodes = nodes
		self.pool.eval = bind(self.pool_eval_embedded, self)
		self.pool.call = bind(self.pool_call_embedded, self)
	else
		self.pool.eval = bind(self.pool_eval_standalone, self)
		self.pool.call = bind(self.pool_call_standalone, self)
	end
end

function M:pool_eval_embedded(...)
	return self._pool:eval_nodes(self._pool_nodes, ...)
end

function M:pool_eval_standalone(...)
	return self._pool:eval(...)
end

function M:pool_call_embedded(...)
	return self._pool:call_nodes(self._pool_nodes, ...)
end

function M:pool_call_standalone(...)
	return self._pool:call(...)
end

function M:_pool_on_connected_one(node)
	log.info('[raft-conn] on_connected_one %s : %s!',node.peer,node.uuid)
	self.nodes_uuid_to_peer[node.uuid] = node.peer
	
	local srv_raft_info = self:get_info(node.uuid)
	if srv_raft_info ~= nil then  -- then node is disconnected
		self.srvs[node.uuid] = {}
		local srv = self.srvs[node.uuid]
		srv.info = srv_raft_info
		
		self:start_state_wait(node.uuid)
		self:node_online(srv)
	end
end

function M:_pool_on_disconnect_one(node)
	log.info('[raft-conn] on_disconnected_one %s : %s!',node.peer,node.uuid)
	
	self.nodes_uuid_to_peer[node.uuid] = nil
	self:node_offline(self.srvs[node.uuid])
	self:stop_status_wait(node.uuid)
	self.srvs[node.uuid] = nil
end


function M:_pool_on_connected()
	log.info('[raft-conn] on_connected all!')
end

function M:_pool_on_disconnect()
	log.info('[raft-conn] on_disconnect all!')
	
end


return M
