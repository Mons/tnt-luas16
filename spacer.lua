
-- USAGE --

--[[
spacer.create_space('space1', {
	{ name='id', type='num' },              -- 1
	{ name='name', type='str' },            -- 2
	{ name='type', type='str' },            -- 3
	{ name='status', type='str' },          -- 4
	{ name='extra', type='*' },             -- 5
	
}, {
	{ name = 'primary', type = 'hash', parts = { 'id' } },
	{ name = 'type', type = 'tree', unique = false, parts = { 'type', 'status' } },
})

spacer.create_space('space2', {
	{ name='id', type='num' },              -- 1
	{ name='name', type='str' },            -- 2
	{ name='type', type='str' },            -- 3
	{ name='status', type='str' },          -- 4
	{ name='extra', type='*' },             -- 5
	
}, {
	{ name = 'primary', type = 'hash', unique = true, parts = { 'id' } },
}, {
	engine = 'sophia'
})

spacer.duplicate_space('space3', 'space1') -- will be identical to space1 (structure + indexes)
spacer.duplicate_space('space4', 'space1', {
	indexes = {
		{ name = 'status', type = 'tree', unique = false, parts = { 'status' } },
	}
}) -- will be identical to space1 (structure + indexes, extra indexes will be created)
spacer.duplicate_space('space5', 'space1', {
	dupindex = false
}) -- will be identical to space1 (only structure, indexes will be omitted)
spacer.duplicate_space('space6', 'space1', {
	dupindex = false,
	indexes = {
		{ name = 'status', type = 'tree', unique = false, parts = { 'status' } },
	}
}) -- will be identical to space1 (only structure, indexes will be omitted, extra indexes will be created)
--]]

local log = require('log')

local F = nil

local function init_tuple_info(space_name, format)
	if F == nil then
		F = {}
	end
	F[space_name] = {}
	F[space_name]['_'] = {}
	for k, v in pairs(format) do
		F[space_name][v.name] = k
		F[space_name]['_'][v.name] = {
			fieldno = k,
			type = v.type
		}
	end
end

local function init_all_spaces_info()
	local spaces = box.space._space:select{}
	for _, sp in pairs(spaces) do
		init_tuple_info(sp[3], sp[7])
	end
end

local function init_indexes(space_name, indexes, keep_obsolete)
	local sp = box.space[space_name]
	
	local created_indexes = {}
	
	-- initializing new indexes
	local name = space_name
	if indexes ~= nil then
		for _, ind in ipairs(indexes) do
			assert(ind.name ~= nil, "Index name cannot be null")
			local ind_opts = {}
			ind_opts.id = ind.id
			ind_opts.type = ind.type
			ind_opts.unique = ind.unique
			ind_opts.if_not_exists = ind.if_not_exists
			
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
			if sp.index[ind.name] ~= nil then
				sp.index[ind.name]:alter(ind_opts)
			else
				sp:create_index(ind.name, ind_opts)
			end
			created_indexes[ind.name] = true
		end
	end
	if not created_indexes['primary'] then
		box.error{reason=string.format("No primary index defined for space '%s'", space_name)}
	end
	
	-- check obsolete indexes in space
	if not keep_obsolete then
		local sp_indexes = box.space._index:select{sp.id}
		local indexes_names = {}
		for _,ind in ipairs(sp_indexes) do
			table.insert(indexes_names, ind[F._index.name])
		end
		for _,ind in ipairs(indexes_names) do
			if ind ~= 'primary' and (not created_indexes['primary'] or not created_indexes[ind]) then
				sp.index[ind]:drop()
			end
		end
		if not created_indexes['primary'] then
			sp.index['primary']:drop()
		end
	end
end

local function create_space(name, format, indexes, opts)
	assert(name ~= nil, "Space name cannot be null")

	local sp = box.space[name]
	if sp == nil or (opts and opts.if_not_exists) then
		sp = box.schema.space.create(name, opts)
	else
		log.info("Space '%s' is already created. Updating indexes and meta information.", name)
	end
	sp:format(format)
	init_tuple_info(name, format)

	init_indexes(name, indexes)
	if not indexes then
		log.warn("No indexes for space '%s' provided.", name)
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
	
	local dupindex = opts['dupindex'] == nil or opts['dupindex'] == true
	local extra_indexes = opts['indexes']
	
	opts['dupindex'] = nil
	opts['indexes'] = nil
	
	local sp = box.space[new_space]
	if sp == nil or opts.if_not_exists then
		sp = box.schema.space.create(new_space, opts)
	else
		log.info("Space '%s' is already created. Updating indexes and meta information.", new_space)
	end
	local format = box.space._space.index.name:get({old_space})[F._space.format]
	
	sp:format(format)
	init_tuple_info(new_space, format)
	
	local new_indexes = {}
	if dupindex then  -- then copy indexes from old_space
		log.info("Duplicating indexes for '%s'", new_space)
		local old_space_id = box.space[old_space].id
		local old_indexes = box.space._index:select({old_space_id})
		
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
	end
	
	if extra_indexes then
		for _,ind in ipairs(extra_indexes) do
			table.insert(new_indexes, ind)
		end
	end
	init_indexes(new_space, new_indexes)
	return sp
end


init_all_spaces_info()
_G.F = F
return {
	F = F,
	create_space = create_space,
	duplicate_space = duplicate_space,
}
