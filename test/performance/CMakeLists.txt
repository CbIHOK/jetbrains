cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")


add_executable( performance EXCLUDE_FROM_ALL
    main.cpp
)

target_link_libraries( performance gtest gtest_main )

install( TARGETS performance DESTINATION . )

#add_test( NAME performance COMMAND regression )
