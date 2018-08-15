cmake_minimum_required (VERSION 3.8)

add_executable( tests 
    main.cpp
    key.cpp
    node_locker.cpp
    virtual_volume.cpp 
    storage.cpp
    bloom.cpp
    )

target_link_libraries( tests SHAtwo gtest gtest_main )

add_test( tests tests )
