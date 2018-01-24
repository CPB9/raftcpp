headers = [
  'raft/Raft.h',
  'raft/Log.h',
  'raft/Node.h',
  'raft/Types.h',
]

src = [
  'raft/Raft.cpp',
  'raft/Log.cpp',
  'raft/Node.cpp',
  'raft/Types.cpp',
]

inc = include_directories('.')

raftcpp_lib = static_library('raftcpp',
  sources : src + headers,
  include_directories : inc,
  dependencies : bmcl_dep,
)

raftcpp_dep = declare_dependency(
  link_with : raftcpp_lib,
  include_directories : inc,
  dependencies : bmcl_dep,
)

