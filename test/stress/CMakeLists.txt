cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4297")


add_executable( stress EXCLUDE_FROM_ALL
    main.cpp
)

target_link_libraries( stress gtest gtest_main )

install( TARGETS stress DESTINATION . )
