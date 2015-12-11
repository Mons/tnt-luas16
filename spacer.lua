
-- USAGE --
-- spacer.create_space('space1', {
-- 	{ name='id', type='num' },              -- 1
-- 	{ name='name', type='str' },            -- 2
-- 	{ name='type', type='str' },            -- 3
-- 	{ name='status', type='str' },          -- 4
-- 	{ name='extra', type='*' },             -- 5
	
-- }, {
-- 	{ name = 'primary', type = 'hash', parts = { 'id' } },
-- 	{ name = 'type', type = 'tree', unique = false, parts = { 'type', 'status' } },
-- })

-- spacer.create_space('space2', {
-- 	{ name='id', type='num' },              -- 1
-- 	{ name='name', type='str' },            -- 2
-- 	{ name='type', type='str' },            -- 3
-- 	{ name='status', type='str' },          -- 4
-- 	{ name='extra', type='*' },             -- 5
	
-- }, {
-- 	{ name = 'primary', type = 'hash', unique = true, parts = { 'id' } },
-- }, {
-- 	engine = 'sophia'
-- })

-- spacer.duplicate_space('space3', 'space1') -- will be identical to space1 (structure + indexes)
-- spacer.duplicate_space('space4', 'space1', {
-- 	indexes = {
-- 		{ name = 'status', type = 'tree', unique = false, parts = { 'status' } },
-- 	}
-- }) -- will be identical to space1 (structure + indexes, extra indexes will be created)
-- spacer.duplicate_space('space5', 'space1', {
-- 	noindex = true
-- }) -- will be identical to space1 (only structure, indexes will be omitted)
-- spacer.duplicate_space('space6', 'space1', {
-- 	noindex = true,
-- 	indexes = {
-- 		{ name = 'status', type = 'tree', unique = false, parts = { 'status' } },
-- 	}
-- }) -- will be identical to space1 (only structure, indexes will be omitted, extra indexes will be created)

local log = require('log')

local FORMAT_KEY = 7

local function init_tuple_info(space_name, format)
	if _G.F == nil then
		_G.F = {}
	end
	_G.F[space_name] = {}
	for k, v in pairs(format) do
		_G.F[space_name][v.name] = k
	end
end

local function init_tuple_info_full(space_name, format)
	if _G.F == nil then
		_G.F = {}
	end
	_G.F[space_name]['_'] = {}
	for k, v in pairs(format) do
		_G.F[space_name]['_'][v.name] = {
			fieldno = k,
			type = v.type
		}
	end
end

local function init_all_spaces_info()
	local spaces = box.space._space:select{}
	for _, sp in pairs(spaces) do
		init_tuple_info(sp[3], sp[7])
		init_tuple_info_full(sp[3], sp[7])
	end
end
init_all_spaces_info()

local function init_indexes(space_name, indexes)
	local sp = box.space[space_name]
	local name = space_name
	for _, ind in ipairs(indexes) do
		assert(ind.name ~= nil, "Index name cannot be null")
		local ind_opts = {}
		if ind.type ~= nil then ind_opts.type = ind.type end
		if ind.unique ~= nil then ind_opts.unique = ind.unique end
		if ind.parts ~= nil then
			ind_opts.parts = {}
			for _, p in ipairs(ind.parts) do
				if F[name][p] ~= nil and F[name]['_'][p] ~= nil then
					table.insert(ind_opts.parts, F[name][p])
					table.insert(ind_opts.parts, F[name]['_'][p].type)
				else
					box.error{reason=string.format("Field %s.%s not found", name, p)}
				end
			end
		end
		sp:create_index(ind.name, ind_opts)
	end
end

local function create_space(name, format, indexes, opts)
	assert(name ~= nil, "Space name cannot be null")

	local sp = box.space[name]
	if sp ~= nil then
		log.info("Space '%s' is already created", name)
		return sp
	end

	init_tuple_info(name, format)
	init_tuple_info_full(name, format)

	sp = box.schema.space.create(name, opts)
	sp:format(format)

	if indexes ~= nil then
		init_indexes(name, indexes)
	end
	return sp
end

function duplicate_space(new_space, old_space, opts)
	assert(new_space ~= nil, "Space name (new_space) cannot be null")
	assert(old_space ~= nil, "Space name (old_space) cannot be null")
	assert(box.space[old_space] ~= nil, "Space " .. old_space .. " does not exist")
	if opts == nil then
		opts = {}
	end
	
	local no_index = opts['noindex'] or false
	local extra_indexes = opts['indexes']
	
	opts['noindex'] = nil
	opts['indexes'] = nil
	
	
	local sp = box.space[new_space]
	if sp ~= nil then
		log.info("Space '%s' is already created", new_space)
		return sp
	end
	
	local format = box.space._space.index.name:get({old_space})[F._space.format]
	
	init_tuple_info(new_space, format)
	init_tuple_info_full(new_space, format)
	
	sp = box.schema.space.create(new_space, opts)
	sp:format(format)
	
	if not no_index then  -- then copy indexes from old_space
		local old_space_id = box.space[old_space].id
		local old_indexes = box.space._index:select({old_space_id})
		
		local new_indexes = {}
		for k1, ind in ipairs(old_indexes) do
			local new_index = {}
			new_index['name'] = ind[F._index.name]
			new_index['type'] = ind[F._index.type]
			new_index['unique'] = ind[F._index.opts]['unique']
			new_index['parts'] = {}
			for _, old_part in ipairs(ind[F._index.parts]) do
				local fieldno = old_part[1] + 1
				table.insert(new_index['parts'], format[fieldno]['name'])
			end
			
			table.insert(new_indexes, new_index)
		end
		init_indexes(new_space, new_indexes)
	end
	
	if extra_indexes then
		init_indexes(new_space, extra_indexes)
	end
	return sp
end


return {
	init_all_spaces_info = init_all_spaces_info,
	create_space = create_space,
	duplicate_space = duplicate_space,
}
