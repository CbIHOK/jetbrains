cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

add_executable( regression
    main.cpp
    merged_string_view.cpp
    path_iterator.cpp
    rare_write_frequent_read_mutex.cpp
    storage.cpp
)

target_link_libraries(
    regression 
    #gtest
    gtest_main
)

install( TARGETS regression DESTINATION . )
