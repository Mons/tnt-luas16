local uuid = require('uuid')
local msgpack = require('msgpack')
local fiber = require('fiber')
local obj = require('obj')
local log = require('log')

local Error = require('Error')
local util = require('util')

local STATUS = {
	R = 'R',
	T = 'T'
}
local DEFAULT_TIMEOUT = 60

local queue = obj.class({}, 'queuelight')

queue.STATUS = STATUS
queue.E = Error{}
queue.E:register({
	{ code = 101, name = 'TASK_NOT_TAKEN', msg = "Task %s not taken by anybody" },
	{ code = 102, name = 'TASK_TAKEN_NOT_BY_YOU', msg = "Task %s taken by %s, not you (%s)" },
	{ code = 103, name = 'TASK_NOT_FOUND', msg = "Task %s not found" },
})

local _wait = {}

function queue:_init(opts)
	util.args_required(opts, {'space', 'index_queue', 'index_primary', 'f_id', 'f_status'})
	self._consumers = {}
	self._taken = {}
	self._wseq = 1

	self.space = opts.space
	self.index_queue = opts.index_queue
	self.index_primary = opts.index_primary
	self.f_id = opts.f_id
	self.f_status = opts.f_status

	assert(box.space[self.space], "Unknown space " .. self.space)
	assert(box.space[self.space].index[self.index_queue], "Unknown queue index " .. self.space .. "." .. self.index_queue)
	assert(box.space[self.space].index[self.index_primary], "Unknown primary index " .. self.space .. "." .. self.index_primary)
end


function queue:wakeup(t)
	if t[self.f_status] ~= STATUS.R then return end
	for _, v in pairs(_wait) do
		v:put(t, 0)
		return
	end
	-- print("No waits")
end

function queue:check_owner(k)
	if not self._taken[k] then
		-- queue.E:raise(queue.E.TASK_NOT_TAKEN, k)
	end
	if self._taken[k] ~= box.session.id() then
		queue.E:raise(queue.E.TASK_TAKEN_NOT_BY_YOU, k, self._taken[k], box.session.id())
	end
	return true
end

function queue:set_status(task_id, status)
	return box.space[self.space]:update({task_id}, {{'=', self.f_status, status}})
end

function queue:release(task_id,opt)
	self:check_owner(task_id)
	opt = opt or {}

	t = self:set_status(task_id, STATUS.R)
	if t and not opt['no_wakeup'] then
		self:wakeup(t)
	end

	local sid = self._taken[task_id]
	self._taken[task_id] = nil
	self._consumers[sid][task_id] = nil
	return t
end

function queue:done(task_id,opt)
	self:check_owner(task_id)
	opt = opt or {}

	t = self:set_status(task_id, STATUS.R)

	local sid = self._taken[task_id]
	self._taken[task_id] = nil
	self._consumers[sid][task_id] = nil
	return t
end

function queue:taken(task)
	local sid = box.session.id()
	if self._consumers[sid] == nil then
		self._consumers[sid] = {}
	end
	local k = task[self.f_id]
	local t = self:set_status(k, STATUS.T)
	
	self._consumers[sid][k] = { util.time(), box.session.peer(sid), t }
	self._taken[k] = sid
	return t
end

function queue:wait(wait_time)
	local wseq = self._wseq
	self._wseq = wseq + 1

	local ch = fiber.channel(1)
	_wait[wseq] = ch
	local t = ch:get(wait_time)
	_wait[wseq] = nil
	return t
end

function queue:on_disconnect(sid)
	local peer = '<PEERNAME>' -- box.session.peer(sid)
	local now = util.time()

	if self._consumers[sid] ~= nil then
		local consumers = self._consumers[sid]
		for k,rec in pairs(consumers) do
			time, peer, task = unpack(rec)

			local v = box.space[self.space].index[self.index_primary]:get({k})

			if v ~= nil and v[self.f_status] == STATUS.T then
				log.info("[ERR] Requeue: %s back to %s by disconnect from %d/%s; taken=%0.6fs", k, STATUS.R, sid, peer, tonumber(now - time))
				v = self:release(v[self.f_id])
			end
		end
		self._consumers[sid] = nil
	end
end


return queue
