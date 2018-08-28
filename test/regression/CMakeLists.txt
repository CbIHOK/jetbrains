cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

add_executable( regression EXCLUDE_FROM_ALL
    main.cpp
    #key.cpp
    #node_locker.cpp
    #virtual_volume.cpp 
    #storage.cpp
    #storage_file.cpp
    #bloom.cpp
    b_tree.cpp
)

target_link_libraries( regression gtest gtest_main )

install( TARGETS regression DESTINATION . )

add_test( NAME regression COMMAND regression )
