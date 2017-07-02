if(MSVC)
    add_definitions(-DRAFT_DLL)
endif()

add_library(raft SHARED
    Raft.h
    Raft.cpp
    Node.h
    Log.h
    Log.cpp
)

if(MINGW)
    target_link_libraries(raft shlwapi)
endif()

if (MSVC)
    target_link_libraries(raft Shlwapi)
endif()
