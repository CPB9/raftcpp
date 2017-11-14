project(raftcpp)

cmake_minimum_required(VERSION 2.8.11)

if(RAFTCPP_FOUND)
    return()
endif()

set(BMCL_NO_QT 1)
add_subdirectory(thirdparty/bmcl EXCLUDE_FROM_ALL)

get_directory_property(HAS_PARENT_SCOPE PARENT_DIRECTORY)
if(HAS_PARENT_SCOPE)
    set(RAFTCPP_FOUND 1 PARENT_SCOPE)
    set(RAFTCPP_FOUND_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}/src
        PARENT_SCOPE
    )
endif()

if(NOT HAS_PARENT_SCOPE)
    add_subdirectory(thirdparty)
endif()

add_library(raftcpp STATIC
    src/raft/Raft.h
    src/raft/Raft.cpp
    src/raft/Log.h
    src/raft/Log.cpp
    src/raft/Node.h
    src/raft/Node.cpp
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

if(NOT HAS_PARENT_SCOPE)
    set(GTEST_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/gtest/include)
    add_subdirectory(tests)
    add_subdirectory(bench)
endif()



