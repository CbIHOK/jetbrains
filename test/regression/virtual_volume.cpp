#include <gtest/gtest.h>
#include <storage.h>


class TestVirtualVolume : public ::testing::Test
{

protected:

    using RetCode = ::jb::RetCode;
    using Storage = ::jb::Storage<>;
    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using VirtualVolume = typename Storage::VirtualVolume;
    using PhysicalVolume = typename Storage::PhysicalVolume;
    using MountPoint = typename Storage::MountPoint;
    using Value = Storage::Value;
    using Timestamp = Storage::Timestamp;
    using Hash = jb::Hash< Policies, Pad, VirtualVolume >;


    ~TestVirtualVolume( ) { Storage::CloseAll( ); }
};


TEST_F( TestVirtualVolume, Dummy )
{
    VirtualVolume v;
    EXPECT_FALSE( v );

    {
        VirtualVolume v1{};
        EXPECT_FALSE( v1 );
        EXPECT_TRUE( v == v1 );
        EXPECT_FALSE( v != v1 );
        EXPECT_FALSE( v < v1 );
        EXPECT_TRUE( v <= v1 );
        EXPECT_FALSE( v > v1 );
        EXPECT_TRUE( v >= v1 );
        EXPECT_EQ( Hash{}( v ), Hash{}( v1 ) );
    }

    {
        auto[ ret ] = v.Insert( "/foo", "boo", Value{} );
        EXPECT_EQ( RetCode::InvalidHandle, ret );
    }

    {
        auto[ ret, value ] = v.Get( "/foo/boo" );
        EXPECT_EQ( RetCode::InvalidHandle, ret );
        EXPECT_EQ( Value{}, value );
    }

    {
        auto[ ret ] = v.Erase( "/foo/boo" );
        EXPECT_EQ( RetCode::InvalidHandle, ret );
    }

    {
        auto[ ret, mp ] = v.Mount( PhysicalVolume{}, "/foo/boo", "/", "boo" );
        EXPECT_EQ( RetCode::InvalidHandle, ret );
        EXPECT_EQ( MountPoint{}, mp );
    }
}


TEST_F( TestVirtualVolume, Base )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    EXPECT_EQ( RetCode::Ok, ret );
    EXPECT_TRUE( v );

    EXPECT_FALSE( v == VirtualVolume{} );
    EXPECT_TRUE( v != VirtualVolume{} );

    {
        auto[ ret, v1 ] = Storage::OpenVirtualVolume( );
        EXPECT_EQ( RetCode::Ok, ret );
        EXPECT_TRUE( v1 );

        EXPECT_FALSE( v == v1 );
        EXPECT_TRUE( v != v1 );
        EXPECT_TRUE( v < v1 || v > v1 );
        EXPECT_TRUE( v <= v1 || v >= v1 );
        EXPECT_NE( Hash{}( v ), Hash{}( v1 ) );

        v1.Close( );
        EXPECT_FALSE( v1 );

        EXPECT_FALSE( v == v1 );
        EXPECT_TRUE( v != v1 );
        EXPECT_TRUE( v < v1 || v > v1 );
        EXPECT_TRUE( v <= v1 || v >= v1 );
        EXPECT_NE( Hash{}( v ), Hash{}( v1 ) );
    }
}


TEST_F( TestVirtualVolume, Limit )
{
    for ( size_t i = 0; i < Policies::VirtualVolumePolicy::VolumeLimit; ++i )
    {
        auto[ ret, volume ] = Storage::OpenVirtualVolume( );
        EXPECT_EQ( RetCode::Ok, ret );
    }

    auto[ ret, volume ] = Storage::OpenVirtualVolume( );
    EXPECT_EQ( RetCode::LimitReached, ret );
    EXPECT_FALSE( volume );
}


TEST_F( TestVirtualVolume, Insert_InvalidKey )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidKey, std::get< RetCode >( v.Insert( "..", "foo", Value{} ) ) );
}


TEST_F( TestVirtualVolume, Insert_InvalidSubkey )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidSubkey, std::get< RetCode >( v.Insert( "/foo", "-foo", Value{} ) ) );
}


TEST_F( TestVirtualVolume, Insert_InvalidLogicalPath )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidLogicalPath, std::get< RetCode >( v.Insert( "/", "foo", Value{} ) ) );
}
