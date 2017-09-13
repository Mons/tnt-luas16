local obj = require('obj')

local M = obj.class({
	_codes = {},
	_msgs = {},
}, 'errorcode')

function M:register_one(err)
	assert(tonumber(err.code) ~= nil, "code is either not provided or not a number")
	assert(err.msg ~= nil, "msg is not provided")
	assert(self[err.code] == nil, string.format("error with code %d is already defined", err.code))
	if err.name ~= nil then
		assert(self[err.name] == nil, string.format("error with name %s is already defined", err.name))
	end
	
	self[err.code] = err.code
	self._codes[err.code] = err.code
	self._msgs[err.code] = err.msg
	if err.name ~= nil then
		self[err.name] = err.code
		self._codes[err.name] = err.code
		self._msgs[err.name] = err.msg
	end
end

function M:register(errors)
	for _, err in pairs(errors) do
		self:register_one(err)
	end
end

function M:raise(code_or_name, ...)
	if code_or_name == nil then
		print("Got an unknown error code or name in raise")
		return
	end
	if self._msgs[code_or_name] == nil then
		print(string.format("Got an unknown error code or name in raise: %d", code_or_name))
		return
	end
	box.error{
		code   = self._codes[code_or_name],
		reason = string.format(self._msgs[code_or_name], ...),
	}
end


return M
