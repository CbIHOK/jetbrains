cmake_minimum_required (VERSION 3.8)

add_executable( tests main.cpp storage.cpp )

target_link_libraries( tests gtest gtest_main )

add_test( tests tests )
