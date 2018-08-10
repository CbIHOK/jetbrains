#include <gtest/gtest.h>
#include <storage.h>

class TestVirtualVolume : public ::testing::Test
{

protected:

    using Storage = ::jb::Storage<>;
    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;

    ~TestVirtualVolume( ) { Storage::CloseAll( );  }
};


TEST_F( TestVirtualVolume, Mount )
{

}
