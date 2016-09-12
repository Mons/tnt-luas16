package = 'tntluas'
version = 'scm-1'
source  = {
    url    = 'git://github.com/igorcoding/tnt-luas16.git',
    branch = 'master',
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
        ['spacer'] = 'spacer.lua',
        ['connpool'] = 'connpool.lua',
        ['errorcode'] = 'errorcode.lua',
        ['queuelight'] = 'queuelight.lua',
    }
}

-- vim: syntax=lua
