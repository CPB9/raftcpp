headers = [
  'raft/Raft.h',
  'raft/Committer.h',
  'raft/Node.h',
  'raft/Error.h',
  'raft/Storage.h',
  'raft/Timer.h',
  'raft/Types.h',
  'raft/Ids.h',
  'raft/Entry.h',
]

src = [
  'raft/Raft.cpp',
  'raft/Committer.cpp',
  'raft/Node.cpp',
  'raft/Error.cpp',
  'raft/Storage.cpp',
  'raft/Timer.cpp',
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

