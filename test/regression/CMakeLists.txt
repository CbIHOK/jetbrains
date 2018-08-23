cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

add_executable( tests 
    main.cpp
    key.cpp
    node_locker.cpp
    virtual_volume.cpp 
    storage.cpp
    storage_file.cpp
    bloom.cpp
)

target_link_libraries( tests SHAtwo gtest gtest_main )

add_test( tests tests )
