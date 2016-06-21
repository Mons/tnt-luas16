local msgpack = require('msgpack')
local fiber = require('fiber')
local Error = require('Error')
local uuid = require('uuid')

local util = {}
util.E = Error{}
util.E:register({
	{ code = 201, name = 'ARG_MISSING', msg = "Field %s is missing" },
	{ code = 202, name = 'ARGS_MISSING', msg = "Arguments are missing" },
	{ code = 203, name = 'ARGS_MISSING_NAME', msg = "Argument '%s' is missing" },
})

function util.time()
	return fiber.time()
end

function util.itime()
	return 0ULL + fiber.time()
end

function util.print_table(table)
	for key, value in pairs(table) do
		print(key, value)
	end
end

function util.get_or_null(table, key)
	if table[key] == nil then
		return msgpack.NULL
	else
		return table[key]
	end
end

function util.get_or(table, key, fallback)
	if table[key] == nil then
		return fallback
	else
		return table[key]
	end
end

function util._arg_required(args, key)
	if args[key] == nil then
		util.E:raise(util.E.ARG_MISSING, key)
	end
	return args[key]
end

function util.arg_required(args, key)
	if args == nil then
		util.E:raise(util.E.ARGS_MISSING)
	end
	util._arg_required(args, key)
end

function util.args_required(args, keys, args_name)
	if args == nil then
		if args_name == nil then
			util.E:raise(util.E.ARGS_MISSING)
		else
			util.E:raise(util.E.ARGS_MISSING_NAME, args_name)
		end
	end
	for k, v in pairs(keys) do
		util._arg_required(args, v)
	end
end

function util.iter(index, ...)
	local f,s,var = index:pairs(...)
	local iterator = {
		f = f,
		s = s,
		var = var,
	}

	local mt = {
		__call = function(t)
			return t.f(t.s, t.var)
		end
	}

	return setmetatable(iterator, mt)
end

function util.uuid()
	return uuid():str()
end

return util
