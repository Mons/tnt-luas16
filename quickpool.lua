local obj = require('obj')
local fiber = require('fiber')
local log = require('log')
local remote = require('net.box')
local msgpack = require('msgpack')

local pool = obj.class({},'pool')

function pool:_init(cfg)
	local zones = {}
	self.total = 0
	self.timeout = cfg.timeout or 1
	self.func_timeout = cfg.func_timeout or 3
	self.name = cfg.name or 'default'
	
	self.raft_enabled = cfg.raft_enabled or false
	self.grouped = cfg.grouped or cfg.raft_enabled or false
	
	self.zones = {}
	if self.grouped then
		if self.raft_enabled then
			self.rafts = {}
		end
		self.peer_groups = {}
		self.peer_groups_counts = {}
		
		for gid, grp in ipairs(cfg.servers) do
			local peers = {}
			local peers_count = 0
			local is_current_in_grp = false
			for _,srv in ipairs(grp) do
				local node = self:init_srv(srv, cfg)
				node.group_id = gid
				
				if self.raft_enabled then
					node.rafted = false
				end
				
				peers[node.peer] = node
				peers_count = peers_count + 1
			end
			table.insert(self.peer_groups, peers)
			table.insert(self.peer_groups_counts, peers_count)
		end
	else
		for _,srv in ipairs(cfg.servers) do
			self:init_srv(srv, cfg)
		end
	end
	
	self.node_by_uuid = {}
	
	self.on_connected_one = pool.on_connected_one
	self.on_connected_zone = pool.on_connected_zone
	self.on_connected = pool.on_connected
	self.on_connfail = pool.on_connfail
	self.on_disconnect_one = pool.on_disconnect_one
	self.on_disconnect_zone = pool.on_disconnect_zone
	self.on_disconnect = pool.on_disconnect
end

function pool:init_srv(srv, cfg)
	local login = srv.login or cfg.login
	local password = srv.password or cfg.password
	
	local uri
	if login and password then
		uri = login .. ':' .. password .. '@' .. srv.uri
	else
		uri = srv.uri
	end
	local zid = srv.zone or cfg.zone or 'default'
	local node = {
		peer = srv.uri,
		uri  = uri,
		zone = zid,
		connected_once = false,
		state = 'inactive',
	}
	if not self.zones[zid] then
		self.zones[zid] = {
			id       = zid,
			total    = 0,
			active   = {}, -- active and responsive connected nodes
			inactive = {}, -- disconnected nodes
			deferred = {}, -- tcp connected but unresponsive nodes
		}
	end

	local zone = self.zones[zid]
	zone.total = zone.total + 1
	self.total = self.total + 1
	table.insert(zone.inactive,node)
	return node
end

function pool:counts()
	if self._counts then return self._counts end
	local active   = 0
	local inactive = 0
	local deferred = 0

	for _,z in pairs(self.zones) do
		active   = active   + #z.active
		inactive = inactive + #z.inactive
		deferred = deferred + #z.deferred
	end
	self._counts = {
		active   = active;
		inactive = inactive;
		deferred = deferred;
	}
	return self._counts
end

function pool:_move_node(zone,node,state1,state2)
	local found = false
	log.info("move node %s from %s to %s", node.peer, state1, state2)
	for _,v in pairs(zone[state1]) do
		if v == node then
			table.remove(zone[state1],_)
			found = true
			break
		end
	end
	if not found then
		log.error("Node %s not dound in state %s.",node.peer,state1)
	end
	table.insert(zone[state2],node)
	node.state = state2
end

function pool:node_state_change(zone,node,state)
	local prevstate = node.state
	self._counts = nil

	if state == 'active' then
		self:_move_node(zone,node,prevstate,state)
		
		if not node.connected_once then
			node.connected_once = true
			self:_on_node_first_connection(node)
		end
		
		self:_on_connected_one(node)
		self.on_connected_one(node)

		if #zone.active == zone.total then
			self.on_connected_zone(zone)
		end
		
		if self:counts().active == self.total then
			self.on_connected()
		end
	else -- deferred or inactive
		self:_move_node(zone,node,prevstate,state)
		-- moving from deferred to inactive we don't alert with callbacks
		if prevstate == 'active' then
			self:_on_disconnect_one(node)
			self.on_disconnect_one(node)

			if #zone.active == 0 then
				self.on_disconnect_zone(zone)
			end

			if self:counts().active == 0 then
				self.on_disconnect()
			end
		end
	end
end

function pool:connect()
	if self.__connecting then return end
	self.__connecting = true
	self:on_init()
	for zid,zone in pairs(self.zones) do
		for _,node in pairs(zone.inactive) do
			fiber.create(function()
				fiber.name(self.name..':'..node.peer)
				node.conn = remote:new( node.uri, { reconnect_after = 1/3, timeout = 1 } )
				local state
				local conn_generation = 0
				while true do
					state = node.conn:_wait_state({active = true})

					local r,e = pcall(node.conn.eval,node.conn,"return box.info")
					if r and e then
						local uuid = e.server.uuid
						local id = e.server.id
						if node.uuid and uuid ~= node.uuid then
							self.node_by_uuid[node.uuid] = nil
							log.warn("server %s changed it's uuid %s -> %s",node.peer,node.uuid,uuid)
						end
						if node.id and id ~= node.id then
							log.warn("server %s changed it's id %s -> %s",node.peer,node.id,id)
						end
						node.uuid = uuid
						node.id = id
						self.node_by_uuid[node.uuid] = node
						conn_generation = conn_generation + 1

						--- TODO: if self then ...

						log.info("connected %s, uuid = %s",node.peer,uuid)
						self:node_state_change(zone,node,'active')

						--- start pinger
						fiber.create(function()
							local gen = conn_generation
							local last_state_ok = true
							while gen == conn_generation do
								--- TODO: replace ping with node status (rw/ro)
								local r,e = pcall(node.conn.ping,node.conn:timeout(self.timeout))
								if r and e then
									if not last_state_ok and gen == conn_generation then
										log.info("node %s become online by ping",node.peer)
										last_state_ok = true
										self:node_state_change(zone,node,'active')
									end
								else
									if last_state_ok and gen == conn_generation then
										log.warn("node %s become offline by ping: %s",node.peer,e)
										last_state_ok = false
										self:node_state_change(zone,node,'deferred')
									end
								end
								fiber.sleep(1)
							end
						end)

						state = node.conn:_wait_state({error = true, closed = true})
						self:node_state_change(zone,node,'inactive')

					else
						log.warn("uuid request failed for %s: %s",node.peer,e)
					end
				end

			end)
		end
	end
end

function pool:_on_node_first_connection(node)
	if not self.raft_enabled then
		return
	end
	
	-- getting group that node is in
	local group_id = node.group_id
	local raft_group = self.peer_groups[group_id]
	
	
	-- determining if raft is already initialized for the selected group
	local is_raft_already_created = node.rafted
	
	if not is_raft_already_created then
		
		-- determining if current node (box.info.server.uuid) is present in selected group
		local current_node_in_group = false
		local all_uuids_nil = true;  -- no uuids except of current's are defined
		for _,n in pairs(raft_group) do
			if n.uuid ~= nil then
				
				if n.uuid ~= node.uuid then
					all_uuids_nil = false;
				end
				
				if n.uuid == box.info.server.uuid then
					current_node_in_group = true
					break
				end
			end
		end
		
		if all_uuids_nil and self.peer_groups_counts[group_id] ~= 1 then
			log.info("Couldn't init raft because of lack of uuid information. node.peer=%s; group_id=%d; current_node_in_group=%s; nodes_count=%d;", node.peer, group_id, current_node_in_group, self.peer_groups_counts[group_id])
			return
		end
		
		log.info("node.peer=%s; group_id=%d; current_node_in_group=%s;", node.peer, group_id, current_node_in_group)
		
		-- collecting all peers into an array
		local peers = {}
		for _,n in pairs(raft_group) do
			table.insert(peers, n.peer)
		end
		
		
		-- initilizing either raftsrv or raftconn
		local raft = nil
		if current_node_in_group then
			local raftsrv = require('raft-srv')
			raft = raftsrv({
				debug = false,
				conn_pool = self,
				servers = peers,
			})
		else
			local raftconn = require('raft-conn')
			raft = raftconn({
				debug = false,
				conn_pool = self,
				servers = peers,
			})
		end
		
		
		-- setting flags that raft is created for each node in the group
		for _,n in pairs(raft_group) do
			n.rafted = true
			n.raft = raft
		end
		
		self.rafts[group_id] = raft
		
		-- starting rafts
		raft:start()
	end

end

function pool:_operation(nodes, operation, ...)
	
	local result_ch = fiber.channel(1)
	local requests_counter = 0
	
	local args = {...}
	local results = {}
	for zid,zone in pairs(self.zones) do
		for _,node in pairs(zone.active) do
			if not nodes or nodes[node.peer] then
				if (node.conn[operation] == nil) then
					log.error("function \'"..operation.."\' not found in net.box connection")
					return
				end
				requests_counter = requests_counter + 1
				fiber.create(function()
					fiber.self():name('fiber_[' .. operation .. '];node.id=' .. node.id)
					local r,e = pcall(node.conn[operation], node.conn:timeout(self.func_timeout), unpack(args))
					if r and e then
						results[node.uuid] = e
					else
						if node.uuid ~= nil then
							results[node.uuid] = msgpack.NULL
						end
						log.error("%s, %s", r, e)
					end
					requests_counter = requests_counter - 1
					if requests_counter == 0 then
						result_ch:put(true)
					end
				end)
			end
		end
	end
	
	local r = result_ch:get(2 * self.func_timeout)
	if r == nil then
		return nil
	end
	
	return results
end


function pool:_operation_on_space(nodes, space, operation, ...)
	
	local result_ch = fiber.channel(1)
	local requests_counter = 0
	
	local args = {...}
	local results = {}
	for zid,zone in pairs(self.zones) do
		for _,node in pairs(zone.active) do
			if not nodes or nodes[node.peer] then
				local conn = node.conn:timeout(self.func_timeout).space[space]
				if (conn[operation] == nil) then
					log.error("function \'"..operation.."\' not found in net.box connection")
					return
				end
				requests_counter = requests_counter + 1
				fiber.create(function()
					fiber.self():name('fiber_[' .. space .. '.' .. operation .. '];node.id=' .. node.id)
					local r,e = pcall(conn[operation], conn, unpack(args))
					if r and e then
						results[node.uuid] = e
					else
						if node.uuid ~= nil then
							results[node.uuid] = msgpack.NULL
						end
						log.error("%s, %s", r, e)
					end
					requests_counter = requests_counter - 1
					if requests_counter == 0 then
						result_ch:put(true)
					end
				end)
			end
		end
	end
	
	local r = result_ch:get(2 * self.func_timeout)
	if r == nil then
		return nil
	end
	
	return results
end

function pool:eval(...)
	return self:_operation(nil, 'eval', ...)
end

function pool:call(...)
	return self:_operation(nil, 'call', ...)
end

function pool:eval_nodes(nodes, ...)
	return self:_operation(nodes, 'eval', ...)
end

function pool:call_nodes(nodes, ...)
	return self:_operation(nodes, 'call', ...)
end

function pool:get_by_uuid(uuid)
	return self.node_by_uuid[uuid]
end

function pool:get_leaders(group_id)
	if self.raft_enabled then
		local leaders_uuids = self.rafts[group_id]:get_leaders_uuids()
		local leaders = {}
		for _, uuid in pairs(leaders_uuids) do
			table.insert(leaders, self:get_by_uuid(uuid))
		end
		return leaders
	else
		return nil
	end
end

function pool:_on_connected_one(node)
	if self.raft_enabled then
		local group_id = node.group_id
		local raft = self.rafts[group_id]
		if raft ~= nil then
			log.info("[_on_connected_one] Calling on node %s", node.peer)
			raft:_pool_on_connected_one(node)
			node.notified_raft_connected = true
			
			for _,n in pairs(self.peer_groups[group_id]) do
				log.info("[_on_connected_one] Trying to call on %s. notified=%s", n.peer, n.notified_raft_connected)
				if n.notified_raft_connected == nil then
					raft:_pool_on_connected_one(n)
					n.notified_raft_connected = nil
				end
			end
		else
			log.warn("[_on_connected_one] Raft for group #%d not found", group_id)
		end
	end
end

function pool:_on_disconnect_one(node)
	if self.raft_enabled then
		local group_id = node.group_id
		local raft = self.rafts[group_id]
		if raft ~= nil then
			log.info("[_on_disconnect_one] Calling on node %s", node.peer)
			raft:_pool_on_disconnect_one(node)
		else
			log.warn("[_on_disconnect_one] Raft for group #%d not found", group_id)
		end
	end
end


function pool.on_connected_one (node)
	log.info('on_connected_one %s : %s',node.peer,node.uuid)
end

function pool.on_connected_zone (zone)
	log.info('on_connected_zone %s',zone.id)
end

function pool.on_connected ()
	log.info('on_connected all')
end

function pool.on_connfail( node )
	log.info('on_connfail ???')
end

function pool.on_disconnect_one (node)
	log.info('on_disconnect_one %s : %s',node.peer,node.uuid)
end

function pool.on_disconnect_zone (zone)
	log.info('on_disconnect_zone %s',zone.id)
end

function pool.on_disconnect ()
	log.info('on_disconnect all')
end

function pool:on_init ()
end

function pool.heartbeat()
	local self = _G['pool']
	-- ...
end

_G['pool'] = pool
return pool
