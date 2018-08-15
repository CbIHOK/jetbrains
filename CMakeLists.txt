cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

add_definitions( -D_SCL_SECURE_NO_WARNINGS )

set(BOOST_INC ../boost_1_67_0)
set(BOOST_LINK ../boost_1_67_0/stage/lib)

add_subdirectory(tools)
enable_testing()
include_directories( ./include ./3rdparty ${BOOST_INC} ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} )

add_subdirectory(3rdparty)
add_subdirectory(test)
