project(raftcpp)

cmake_minimum_required(VERSION 2.8.11)

include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/bmcl/cmake/Bmcl.cmake)

enable_testing()

if(RAFTCPP_FOUND)
    return()
endif()

get_directory_property(HAS_PARENT_SCOPE PARENT_DIRECTORY)
if(HAS_PARENT_SCOPE)
    set(RAFTCPP_FOUND 1 PARENT_SCOPE)
    set(RAFTCPP_FOUND_INCLUDE_DIRS
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}/src
        PARENT_SCOPE
    )
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")

set(BMCL_NO_QT 1)
bmcl_add_dep_gtest(thirdparty/gtest)
bmcl_add_dep_bmcl(thirdparty/bmcl)

bmcl_add_library(raftcpp STATIC
    src/raft/Ids.h
    src/raft/Entry.h
    src/raft/Raft.h
    src/raft/Raft.cpp
    src/raft/Log.h
    src/raft/Log.cpp
    src/raft/Node.h
    src/raft/Node.cpp
    src/raft/Types.h
    src/raft/Types.cpp
    src/raft/Timer.h
    src/raft/Timer.cpp
)

target_include_directories(raftcpp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(raftcpp
    bmcl
)

if(NOT HAS_PARENT_SCOPE)
    add_subdirectory(tests)
endif()

