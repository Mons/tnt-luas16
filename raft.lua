------- Usage example: -------
-- local raft = require('raft')
-- r = raft({
-- 	login = 'repler',
-- 	password = 'repler',
-- 	servers = {
-- 		{ uri = '127.0.0.1:3313' },
-- 		{ uri = '127.0.0.2:3313' },
-- 	},
-- })

-- r:start()


local cp = require('quickpool')
local log = require('log')
local uuid = require('uuid')
local fiber = require('fiber')
local obj = require('obj')

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

local MS_TO_S = 1/1000 * 10 -- FIXME

local M = obj.class({}, 'Raft')

function M:_init(cfg)
	self._election_fiber = nil
	self._election_ch = fiber.channel(1)
	self._current_election_timeout = 0
	self._election_timer_active = false
	
	self._heartbeat_fiber = nil
	self.HEARTBEAT_PERIOD = 100 * MS_TO_S
	
	self._pool = cp {
		name = 'raft-pool',
		login = cfg.login,
		password = cfg.password,
		servers = cfg.servers,
	}
	
	self._pool.on_connected = bind(self._pool_on_connected, self)
	self._pool.on_connected_one = bind(self._pool_on_connected_one, self)
	self._pool.on_disconnect = bind(self._pool_on_disconnect, self)
	
	_G.raft = {
		request_vote = bind(self.request_vote, self),
		heartbeat = bind(self.heartbeat, self),
		get_leader = bind(self.get_leader, self),
	}
	
	self.S = {
		FOLLOWER = 'follower',
		CANDIDATE = 'candidate',
		LEADER = 'leader'
	}
	
	self._nodes_count = #cfg.servers
	self._id = box.info.server.id
	self._uuid = box.info.server.uuid  -- TODO: need to have both id and uuid perhaps
	self._state = self.S.FOLLOWER
	self._term = 0
	self._vote_count = 0
	self._leader = nil
end

function M:start()
	self._pool:connect()
	self:start_election_timer()
end

function M:is_leader()
	return self._leader == self._id
end

function M:get_leader()
	return self._leader
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
	self._election_ch:put(1)
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

function M:_initiate_elections()
	self._term = self._term + 1
	self._state = self.S.CANDIDATE
	
	local r = self._pool:call("raft.request_vote", self._term, self._id, "ABRACADABRA")
	for k,v in pairs(r) do print(k, v[1]) end
	-- finding majority
	for node_id,response in pairs(r) do
		local decision = response[1][1]
		
		local vote = decision == "ack" and 1 or 0
		self._vote_count = self._vote_count + vote
	end
	
	log.info(string.format("resulting votes count: %d/%d", self._vote_count, self._nodes_count))
	
	if self._vote_count > self._nodes_count / 2 then
		-- elections won
		log.info(string.format("node %d won elections", self._id))
		self._state = self.S.LEADER
		self._leader = self._id
		self._vote_count = 0
		self._election_timer_active = false
		self:start_heartbeater()
	else
		-- elections lost
		log.info(string.format("node %d lost elections", self._id))
		self._state = self.S.FOLLOWER
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
	if self._heartbeat_fiber then
		self._heartbeat_fiber:cancel()
		self._heartbeat_fiber = nil
	end
end

function M:restart_heartbeater()
	self:stop_heartbeater()
	self:start_heartbeater()
end

function M:_heartbeater()
	while true do
		log.info("performing heartbeat")
		local r = self._pool:call("raft.heartbeat", self._term, self._id, self._leader)
		fiber.sleep(self.HEARTBEAT_PERIOD)
	end
end

---------------- incoming ----------------

function M:request_vote(term, id, candidate)
	self:reset_election_timer()
	local res
	if self._id == id or self._term < term then  -- newer one
		self._term = term
		res = "ack"
	else
		res = "nack"
	end
	log.info(string.format("--> request_vote: term = %d; id = %d; res = %s", term, id, res))
	return res
end

function M:heartbeat(term, id, leader_id)
	local res = "ack"
	if self._id ~= id and self._term <= term then
		log.info(string.format("--> heartbeat: term = %d; id = %d; leader_id = %d; res = %s", term, id, leader_id, res))
		self:reset_election_timer()
		self._term = term
		self._leader = leader_id
	end
	return res
end

---------------- pool functions ----------------

function M:_pool_on_connected_one(node)
	log.info('on_connected_one %s : %s!',node.peer,node.uuid)
	-- if node.id ~= self._id then
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

