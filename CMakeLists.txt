cmake_minimum_required (VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


add_definitions( -D_SCL_SECURE_NO_WARNINGS )

if ( WIN32 )
    add_definitions( -DUSE_OS_POLICY=WindowsPolicy )
    add_definitions( -D_ENABLE_EXTENDED_ALIGNED_STORAGE )
    #add_definitions( -DDEBUG_ )
endif ( WIN32 )


set(BOOST_INC ../boost_1_67_0)
set(BOOST_LINK ../boost_1_67_0/stage/lib)


file(MAKE_DIRECTORY ./build)


add_subdirectory(tools)


enable_testing()


include_directories( ./include ./3rdparty ${BOOST_INC} ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} )
link_directories( ${BOOST_LINK} )


set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/build)


add_subdirectory(test)

