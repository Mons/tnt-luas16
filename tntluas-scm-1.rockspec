package = 'tntluas'
version = 'scm-1'
source  = {
    url    = 'git://github.com/igorcoding/tnt-luas16.git',
    branch = 'queuelight',
}
description = {
    summary  = "Various useful tarantool luas",
    homepage = 'https://github.com/igorcoding/tnt-luas16',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',
    modules = {
        ['connpool'] = 'connpool.lua',
        ['errorcode'] = 'errorcode.lua',
        ['queuelight'] = 'queuelight.lua',
        ['tntutil'] = 'tntutil.lua',
    }
}

-- vim: syntax=lua
