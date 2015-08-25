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


local cp = require('quickpool')
local log = require('log')
local uuid = require('uuid')
local fiber = require('fiber')
local obj = require('obj')
local yaml = require('yaml')

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


--------- elections ---------

local MS_TO_S = 1/1000 * 10 -- FIXME: (1/1000)

local M = obj.class({}, 'Raft')

function M:_init(cfg)
	self.name = cfg.name or 'default'
	self.debug = cfg.debug or false
	
	self._election_fiber = nil
	self._election_ch = fiber.channel(1)
	self._current_election_timeout = 0
	self._election_timer_active = false
	
	self._heartbeat_fiber = nil
	self._heartbeater_active = false
	self.HEARTBEAT_PERIOD = 100 * MS_TO_S
	
	self._pool = cp {
		name = 'raft-'.. self.name..'-pool',
		login = cfg.login,
		password = cfg.password,
		servers = cfg.servers,
	}
	self._pool.on_connected = bind(self._pool_on_connected, self)
	self._pool.on_connected_one = bind(self._pool_on_connected_one, self)
	self._pool.on_disconnect = bind(self._pool_on_disconnect, self)
	
	self.FUNC = self:_make_global_funcs({
		'request_vote',
		'heartbeat',
		'is_leader',
		'get_leader',
		'get_info',
		'state_wait',
	})
	
	self.S = {
		FOLLOWER = 'follower',
		CANDIDATE = 'candidate',
		LEADER = 'leader'
	}
	
	self._nodes_count = #cfg.servers
	self._id = box.info.server.id
	self._uuid = box.info.server.uuid
	self._prev_state = nil
	self._state = self.S.FOLLOWER
	self._term = 0
	self._vote_count = 0
	self._leader = nil
	
	self._state_channels = setmetatable({}, {__mode='kv'})
	
	self._debug_fiber = nil
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
	if new_state ~= self._prev_state then
		self._prev_state = self._state
		self._state = new_state
		for _,v in pairs(self._state_channels) do
			v:put(true)
		end
	end
end

function M:start()
	self._pool:connect()
	self:start_election_timer()
	
	if self.debug then
		self:start_debugger()
	end
end

function M:stop()
	self:stop_election_timer()
	self:stop_heartbeater()
	self:stop_debugger()
	-- TODO: self._pool:disconnect()
	
	_G.raft[self.name] = nil
end

function M:_new_election_timeout()
	self._current_election_timeout = math.random(150, 300) * MS_TO_S
	return self._current_election_timeout
end

function M:start_election_timer()
	if self._election_fiber == nil then
		self._election_fiber =
			fiber.create(bind(self._election_timer, self))
		self._election_fiber:name('election_fiber')
	end
end

function M:stop_election_timer()
	if self._election_fiber then
		self._election_fiber:cancel()
		self._election_fiber = nil
	end
end

function M:restart_election_timer()
	self:stop_election_timer()
	self:start_election_timer()
end

function M:reset_election_timer()
	if self._election_timer_active then
		self._election_ch:put(1)
	end
end

function M:_election_timer()
	self._election_timer_active = true
	while self._election_timer_active do
		local v = self._election_ch:get(self:_new_election_timeout())
		if v == nil then
			log.info("Timeout exceeded. Starting elections.")
			self:_initiate_elections()
		end
	end
end

function M:_is_good_for_candidate()
	-- TODO: check all nodes and see if current node is good to be a leader, then return true
	-- TODO: if not, return false
	
	local r = self._pool:eval("return box.info")
	if not r then return true end
	
	-- for now it is that lag is the least
	local minimum = {
		uuid = -1,
		lag = nil
	}
	for _,resp in pairs(r) do
		log.info("[lag] id = %d; lag = %d", resp.server.id, resp.replication.lag)
		if minimum.lag == nil or (resp.replication.lag <= minimum.lag and resp.server.uuid == self._uuid) or resp.replication.lag < minimum.lag then
			minimum.uuid = resp.server.uuid
			minimum.lag = resp.replication.lag
		end
	end
	log.info("[lag] minimum = {uuid=%s; lag=%d}", minimum.uuid, minimum.lag)
	
	return minimum.uuid == self._uuid
	-- return true
end

function M:_initiate_elections()
	if not self:_is_good_for_candidate() then
		log.info("node %d is not good to be a candidate")
		return
	end
	
	self._term = self._term + 1
	self:_set_state(self.S.CANDIDATE)
	
	local r = self._pool:call(self.FUNC.request_vote, self._term, self._uuid)
	if not r then return end
	
	-- print(yaml.encode(r))
	for k,v in pairs(r) do print(k, v[1]) end
	-- finding majority
	for _,response in pairs(r) do
		local decision = response[1][1]
		
		local vote = decision == "ack" and 1 or 0
		self._vote_count = self._vote_count + vote
	end
	
	log.info("resulting votes count: %d/%d", self._vote_count, self._nodes_count)
	
	if self._vote_count > self._nodes_count / 2 then
		-- elections won
		log.info("node %d won elections [uuid = %s]", self._id, self._uuid)
		self:_set_state(self.S.LEADER)
		self._leader = { id=self._id, uuid=self._uuid }
		self._vote_count = 0
		self._election_timer_active = false
		self:start_heartbeater()
	else
		-- elections lost
		log.info("node %d lost elections [uuid = %s]", self._id, self._uuid)
		self:_set_state(self.S.FOLLOWER)
		self._leader = nil
		self._vote_count = 0
		self._election_timer_active = true
	end
end

function M:start_heartbeater()
	if self._heartbeat_fiber == nil then
		self._heartbeat_fiber = fiber.create(bind(self._heartbeater, self))
		self._heartbeat_fiber:name("heartbeat_fiber")
	end
end

function M:stop_heartbeater()
	-- print("---- stopping heartbeater 1")
	if self._heartbeat_fiber then
		-- print("---- stopping heartbeater 2")
		self._heartbeater_active = false
		self._heartbeat_fiber:cancel()
		self._heartbeat_fiber = nil
	end
end

function M:restart_heartbeater()
	self:stop_heartbeater()
	self:start_heartbeater()
end

function M:_heartbeater()
	self._heartbeater_active = true
	while self._heartbeater_active do
		log.info("performing heartbeat")
		local r = self._pool:call(self.FUNC.heartbeat, self._term, self._uuid, self._leader)
		fiber.sleep(self.HEARTBEAT_PERIOD)
	end
end

function M:start_debugger()
	if self._debug_fiber == nil then
		self._debug_active = true
		self._debug_fiber = fiber.create(function()
			while self._debug_active do
				fiber.self():name("debug_fiber")
				log.info("state=%s; term=%d; id=%d; uuid=%s; leader=%s", self._state, self._term, self._id, self._uuid, self._leader and self._leader.uuid)
				fiber.sleep(5)
			end
		end)
	end
end

function M:stop_debugger()
	if self._debug_fiber then
		self._debug_active = false
		self._debug_fiber:cancel()
		self._debug_fiber = nil
	end
end

---------------- Global functions ----------------

function M:request_vote(term, uuid)
	self:reset_election_timer()
	local res
	if self._uuid == uuid or self._term < term then  -- newer one
		self._term = term
		res = "ack"
	else
		res = "nack"
	end
	log.info("--> request_vote: term = %d; uuid = %s; res = %s", term, uuid, res)
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
		self._leader = leader
		log.info("--> heartbeat: term = %d; uuid = %s; leader_id = %d; res = %s", term, uuid, leader.id, res)
	end
	return res
end

function M:is_leader()
	return self._leader.uuid == self._uuid
end

function M:get_leader()
	return self._leader
end

function M:get_info()
	return {
		id = self._id,
		uuid = self._uuid,
		state = self._state,
		leader = self._leader,
	}
end

function M:state_wait(timeout)
	timeout = tonumber(timeout) or 0
	
	local ch = fiber.channel(1)
	self._state_channels[ch] = ch
	local m = ch:get(timeout)
	self._state_channels[ch] = nil
	
	
	
	return {
		state = self._state,
	}
end

---------------- pool functions ----------------

function M:_pool_on_connected_one(node)
	log.info('on_connected_one %s : %s!',node.peer,node.uuid)
	-- if node.id ~= self._id and not self._heartbeater_active then
	-- 	self:start_election_timer()
	-- end
end

function M:_pool_on_connected()
	log.info('on_connected all!')
end

function M:_pool_on_disconnect()
	log.info('on_disconnect all!')
	
end


return M

