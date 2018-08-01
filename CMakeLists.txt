cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(BOOST_INC ../boost_1_67_0)
set(BOOST_LINK ../boost_1_67_0/stage/lib)

include_directories(
	./include
	${BOOST_INC}
)

add_subdirectory(test)