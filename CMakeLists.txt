project(raftcpp)

cmake_minimum_required(VERSION 2.8.11)

set(BMCL_NO_QT 1)
add_subdirectory(thirdparty/bmcl)
add_subdirectory(thirdparty/gtest)

add_library(raftcpp STATIC
    src/raft/Raft.h
    src/raft/Raft.cpp
    src/raft/Log.h
    src/raft/Log.cpp
    src/raft/Node.h
    src/raft/Types.h
)
target_include_directories(raftcpp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(raftcpp
    bmcl
)

if(NOT MSVC)
    set(RAFTCPP_DEFS -std=c++11)
endif()

target_compile_options(raftcpp PUBLIC ${RAFTCPP_DEFS})

set(GTEST_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/gtest/include)
add_subdirectory(tests)


