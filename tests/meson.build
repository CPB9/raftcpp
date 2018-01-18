test_inc = include_directories('.')

mock_lib = static_library('raftcpp-test-mocks',
  sources : ['mock_send_functions.h', 'mock_send_functions.cpp'],
  include_directories : test_inc,
    dependencies : raftcpp_dep,
)

tests = [
  ['test-log', 'test_log.cpp'],
  ['test-node', 'test_node.cpp'],
  ['test-scenario', 'test_scenario.cpp'],
  ['test-server', 'test_server.cpp'],
]

foreach t : tests
  name = 'raftcpp-' + t[0]
  exe = executable(name,
    sources : t[1],
    dependencies : [raftcpp_dep, gtest_dep, gtest_main_dep],
    link_with : mock_lib,
    include_directories : test_inc,
  )
  test(name, exe)
endforeach
