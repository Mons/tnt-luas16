------- Usage example: -------
-- local raft = require('raft')
-- r = raft({
-- 	login = 'repler',
-- 	password = 'repler',
-- 	debug = true,
-- 	servers = {
--		{ uri = '127.0.0.1:3303' },
--		{ uri = '127.0.0.2:3303' },
--		{ uri = '127.0.0.3:3303' },
--		{ uri = '127.0.0.4:3303' },
-- 	},
-- })

-- r:start()


local obj = require('obj')
local log = require('log')
local fiber = require('fiber')
local msgpack = require('msgpack')

local function bind(func, object)
    return function(...) return func(object, ...) end
end

--------- Hooker ---------

local Hooker = obj.class({
	hooks = {},
	__call = function(self, ...)
		for _,h in ipairs(self.hooks) do
			h(...)
		end
	end
}, 'Hooker')

function Hooker:add(hook)
	self.hooks[#self.hooks + 1] = hook
end


--------- raft ---------

local MS_TO_S = 1/1000

local raft = require('raft')
local M = obj.class({}, 'raft-srv', raft)

local _local = {
	election_fiber = nil,
	election_ch = fiber.channel(1),
	
	heartbeat_fiber = nil,
	
	state_channels = setmetatable({}, {__mode='kv'}),
	debug_fiber = nil,
}

function M:_init(cfg)
	self.name = cfg.name or 'default'
	self.debug = cfg.debug or false
	
	self._current_election_timeout = 0
	self._election_timer_active = false
	
	self._heartbeater_active = false
	self.HEARTBEAT_PERIOD = 100 * MS_TO_S
	
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
	
	self.FUNC = self:_make_global_funcs({
		'request_vote',
		'heartbeat',
		'is_leader',
		'get_leader',
		'info',
		'state_wait',
	})
	
	self.S = {
		IDLE = 'idle',
		FOLLOWER = 'follower',
		CANDIDATE = 'candidate',
		LEADER = 'leader'
	}
	
	self._nodes_count = #cfg.servers
	self._active_nodes_count = 0
	self._id = box.info.server.id
	self._uuid = box.info.server.uuid
	self._prev_state = nil
	self._state = self.S.IDLE
	self._term = 0
	self._vote_count = 0
	self._prev_leader = msgpack.NULL
	self._leader = msgpack.NULL
	self._preferred_leader_uuid = msgpack.NULL
	
	self._debug_active = false
end

function M:_make_global_funcs(func_names)
	local F = {}
	if _G.raft == nil then
		_G.raft = {}
	end
	if _G.raft[self.name] ~= nil then
		log.warn("Another raft." .. self.name .. " callbacks detected in _G. Replacing them.")
	end
	_G.raft[self.name] = {}
	for _,f in ipairs(func_names) do
		if self[f] then
			_G.raft[self.name][f] = bind(self[f], self)
			F[f] = 'raft.' .. self.name .. '.' .. f
		else
			log.warn("No function '" .. f .. "' found. Skipping...")
		end
	end
	return F
end

function M:_set_state(new_state)
	if new_state ~= self._state then
		self._prev_state = self._state
		self._state = new_state
		log.info("State: %s -> %s", self._prev_state, self._state)
		for _,v in pairs(_local.state_channels) do
			v:put('state_change')
		end
	end
end

function M:_set_leader(new_leader)
	if new_leader == nil or self._leader == nil or new_leader.uuid ~= self._leader.uuid then
		self._prev_leader = self._leader
		self._leader = new_leader
		
		if self._prev_leader ~= nil and self._leader ~= nil then
			for _,v in pairs(_local.state_channels) do
				v:put('leader_change')
			end
		end
	end
	
end

function M:start()
	if self.mode == self.MODES.STANDALONE then
		self._pool:connect()
	end
	self:start_election_timer()
	
	if self.debug then
		self:start_debugger()
	end
	log.info("Started raft")
end

function M:stop()
	self:stop_election_timer()
	self:stop_heartbeater()
	self:stop_debugger()
	if self.mode == self.MODES.STANDALONE then
		-- self._pool:disconnect()
	end
	
	_G.raft[self.name] = nil
end

function M:_new_election_timeout()
	self._current_election_timeout = math.random(150, 300) * MS_TO_S
	return self._current_election_timeout
end

function M:start_election_timer()
	if _local.election_fiber == nil then
		_local.election_fiber =
			fiber.create(bind(self._election_timer, self))
		_local.election_fiber:name('election_fiber')
	end
end

function M:stop_election_timer()
	if self._election_fiber then
		_local.election_fiber:cancel()
		_local.election_fiber = nil
	end
end

function M:restart_election_timer()
	self:stop_election_timer()
	self:start_election_timer()
end

function M:reset_election_timer()
	if self._election_timer_active then
		_local.election_ch:put(1)
	end
end

function M:_election_timer()
	self._election_timer_active = true
	while self._election_timer_active do
		local v = _local.election_ch:get(self:_new_election_timeout())
		if v == nil then
			if self.debug then log.info("Timeout exceeded. Starting elections.") end
			self:_initiate_elections()
		end
	end
end

function M:_is_good_for_candidate()
	-- TODO: check all nodes and see if current node is good to be a leader, then return true
	-- TODO: if not, return false
	
	local r = self.pool.eval("return box.info")
	if not r then return true end
	
	-- for now it is that lag is the least
	local minimum = {
		uuid = self._uuid,
		lag = nil
	}
	for _,resp in pairs(r) do
		if resp.replication.status ~= 'off' and resp.replication.lag ~= nil then
			if self.debug then log.info("[lag] id = %d; uuid = %s; lag = %f", resp.server.id, resp.server.uuid, resp.replication.lag) end
			if self.debug then
				log.info("[lag] condition1: %d", minimum.lag == nil and 1 or 0)
				if minimum.lag ~= nil then
					log.info("[lag] condition2: %d", (resp.replication.lag <= minimum.lag and resp.server.uuid == self._uuid) and 1 or 0)
					log.info("[lag] condition3: %d", resp.replication.lag < minimum.lag and 1 or 0)
				end
			end
			if minimum.lag == nil or (resp.replication.lag <= minimum.lag and resp.server.uuid == self._uuid) or resp.replication.lag < minimum.lag then
				minimum.uuid = resp.server.uuid
				minimum.lag = resp.replication.lag
			end
		end
	end
	if self.debug then
		if minimum.lag ~= nil then
			log.info("[lag] minimum = {uuid=%s; lag=%d}", minimum.uuid, minimum.lag)
		else
			log.info("[lag] lag couldn't been determined. uuid = ", minimum.uuid)
		end
	end
	
	-- self._preferred_leader_uuid = minimum.uuid
	return minimum.uuid == self._uuid
end

function M:_initiate_elections()
	if not self:_is_good_for_candidate() then
		if self.debug then log.info("node %s is not good to be a candidate", self._uuid) end
		return
	else
		if self.debug then
			log.info("node %s is good to be a candidate. Active nodes = %d. Nodes count = %d", self._uuid, self._active_nodes_count, self._nodes_count)
		end
	end
	
	self:_set_leader(msgpack.NULL)
	
	if self._nodes_count ~= 1 and self._active_nodes_count == 1 then
		log.info("node %s is left by itself", self._uuid)
		self:_set_state(self.S.IDLE)
		return
	end
	
	self._term = self._term + 1
	self:_set_state(self.S.CANDIDATE)
	
	local r = self.pool.call(self.FUNC.request_vote, self._term, self._uuid)
	if not r then return end
	
	if self.debug then for k,v in pairs(r) do print(k, v[1]) end end
	-- finding majority
	for _,response in pairs(r) do
		local decision = response[1][1]
		
		local vote = decision == "ack" and 1 or 0
		self._vote_count = self._vote_count + vote
	end
	
	if self.debug then log.info("resulting votes count: %d/%d", self._vote_count, self._nodes_count) end
	
	if self._vote_count > self._nodes_count / 2 then
		-- elections won
		if self.debug then log.info("node %d won elections [uuid = %s]", self._id, self._uuid) end
		self:_set_state(self.S.LEADER)
		self:_set_leader({ id=self._id, uuid=self._uuid })
		self._vote_count = 0
		self._election_timer_active = false
		self:start_heartbeater()
	else
		-- elections lost
		if self.debug then log.info("node %d lost elections [uuid = %s]", self._id, self._uuid) end
		self:_set_state(self.S.IDLE)
		self:_set_leader(msgpack.NULL)
		self._vote_count = 0
		self._election_timer_active = true
	end
	-- self._preferred_leader_uuid = msgpack.NULL;
end

function M:start_heartbeater()
	if _local.heartbeat_fiber == nil then
		_local._heartbeat_fiber = fiber.create(bind(self._heartbeater, self))
		_local._heartbeat_fiber:name("heartbeat_fiber")
	end
end

function M:stop_heartbeater()
	-- print("---- stopping heartbeater 1")
	if _local._heartbeat_fiber then
		-- print("---- stopping heartbeater 2")
		self._heartbeater_active = false
		_local._heartbeat_fiber:cancel()
		_local._heartbeat_fiber = nil
	end
end

function M:restart_heartbeater()
	self:stop_heartbeater()
	self:start_heartbeater()
end

function M:_heartbeater()
	self._heartbeater_active = true
	while self._heartbeater_active do
		if self.debug then log.info("performing heartbeat") end
		local r = self.pool.call(self.FUNC.heartbeat, self._term, self._uuid, self._leader)
		fiber.sleep(self.HEARTBEAT_PERIOD)
	end
end

function M:start_debugger()
	local logger = function()
		local s = "state=%s; term=%d; id=%d; uuid=%s; leader=%s"
		local _nil = "nil"
		local leader_str = _nil
		if self._leader ~= nil then
			leader_str = self._leader.uuid
		end
		return string.format(s, self._state or _nil,
								self._term or _nil,
								self._id or _nil,
								self._uuid or _nil,
								leader_str)
	end
	if _local.debug_fiber == nil then
		self._debug_active = true
		_local._debug_fiber = fiber.create(function()
			fiber.self():name("debug_fiber")
			while self._debug_active do
				log.info(logger())
				fiber.sleep(5)
			end
		end)
	end
end

function M:stop_debugger()
	if _local.debug_fiber then
		self._debug_active = false
		_local.debug_fiber:cancel()
		_local.debug_fiber = nil
	end
end

function M:call_on_leader(func_name, ...)
	local c = self._pool:get_by_uuid(self._leader.uuid)
	return c:call(func_name, ...)
end

---------------- Global functions ----------------

function M:request_vote(term, uuid)
	self:reset_election_timer()
	
	local res
	if self._preferred_leader_uuid ~= msgpack.NULL then
		if self._preferred_leader_uuid == uuid then
			res = "ack"
		else
			res = "nack"
		end
	else
		if self._uuid == uuid or self._term < term then  -- newer one
			res = "ack"
		else
			res = "nack"
		end
	end
	
	if self.debug then log.info("--> request_vote: term = %d; uuid = %s; res = %s", term, uuid, res) end
	return res
end

function M:heartbeat(term, uuid, leader)
	local res = "ack"
	if self._uuid ~= uuid and self._term <= term then
		self:start_election_timer()
		self:reset_election_timer()
		self:stop_heartbeater()
		self:_set_state(self.S.FOLLOWER)
		self._vote_count = 0
		self._term = term
		self:_set_leader(leader)
		self._preferred_leader_uuid = msgpack.NULL
		if self.debug then log.info("--> heartbeat: term = %d; uuid = %s; leader_id = %d; res = %s", term, uuid, leader.id, res) end
	end
	return res
end

function M:is_leader()
	return self._leader.uuid == self._uuid
end

function M:get_leader()
	return self._leader
end

function M:get_leaders_uuids()
	return { self._leader and self._leader.uuid }
end

function M:info()
	return {
		id = self._id,
		uuid = self._uuid,
		state = self._state,
		leader = self._leader,
	}
end

function M:state_wait(timeout)
	timeout = tonumber(timeout) or 0
	
	local ch = fiber.channel(5)
	_local.state_channels[ch] = ch
	local m = ch:get(timeout)
	_local.state_channels[ch] = nil
	
	local event = 'none'
	if m ~= nil then
		event = m
	end
	
	if self.debug then log.info("--> state_wait()") end
	return {
		event = event,
		state = self._state,
		prev_state = self._prev_state,
		info = self:info(),
	}
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
	log.info('[raft-srv] on_connected_one %s : %s!',node.peer,node.uuid)
	self._active_nodes_count = self._active_nodes_count + 1
end

function M:_pool_on_disconnect_one(node)
	log.info('[raft-srv] on_disconnect_one %s : %s!',node.peer,node.uuid)
	self._active_nodes_count = self._active_nodes_count - 1
end

function M:_pool_on_connected()
	log.info('[raft-srv] on_connected all!')
end

function M:_pool_on_disconnect()
	log.info('[raft-srv] on_disconnect all!')
	
end


return M

