bmcl_add_library(raftcpp-mocks
    mock_send_functions.h mock_send_functions.cpp
)

target_link_libraries(raftcpp-mocks raftcpp)

macro(add_unit_test test file)
    bmcl_add_unit_test(raftcpp-test-${test} ${file})
    target_link_libraries(raftcpp-test-${test}
        raftcpp-mocks
        raftcpp
    )
endmacro()

add_unit_test(log test_log.cpp)
add_unit_test(node test_node.cpp)
add_unit_test(scenario test_scenario.cpp)
add_unit_test(server test_server.cpp)

