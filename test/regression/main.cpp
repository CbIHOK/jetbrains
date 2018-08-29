#include <gtest/gtest.h>
#include <windows_policy.h>

namespace jb
{
    uint64_t WindowsPolicy::_offset = 0;
}


int main( int argc, char **argv )
{
    ::testing::InitGoogleTest( &argc, argv );
    int result = RUN_ALL_TESTS( );
    return result;
}
