#include <gtest/gtest.h>
#include <storage.h>


class TestVirtualVolume : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using Storage = ::jb::Storage<Policies, Pad>;
    using RetCode = typename Storage::RetCode;
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


TEST_F( TestVirtualVolume, Get_InvalidKey )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidKey, std::get< RetCode >( v.Get( "\\" ) ) );
}


TEST_F( TestVirtualVolume, Get_InvalidLogicalPath )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidLogicalPath, std::get< RetCode >( v.Get( "/foo/boo" ) ) );
}


TEST_F( TestVirtualVolume, Erase_InvalidKey )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidKey, std::get< RetCode >( v.Erase( "sfhgsg" ) ) );
}


TEST_F( TestVirtualVolume, Erase_InvalidLogicalPath )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume( );
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidLogicalPath, std::get< RetCode >( v.Erase( "/foo/boo" ) ) );
}


TEST_F( TestVirtualVolume, Mount_InvalidPhysicalVolume )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidHandle, std::get< RetCode >( v.Mount( PhysicalVolume{}, "/foo/boo", "/", "boo" ) ) );
}


TEST_F( TestVirtualVolume, Mount_InvalidPhysicalPath )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidKey, std::get< RetCode >( v.Mount( PhysicalVolume{}, "foo", "/", "boo" ) ) );
}


TEST_F( TestVirtualVolume, Mount_InvalidLogicalPath )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidKey, std::get< RetCode >( v.Mount( PhysicalVolume{}, "/foo", "/-boo", "boo" ) ) );
}


TEST_F( TestVirtualVolume, Mount_InvalidAlias )
{
    auto[ ret, v ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, ret );
    EXPECT_EQ( RetCode::InvalidSubkey, std::get< RetCode >( v.Mount( PhysicalVolume{}, "/", "/", "/boo" ) ) );
}


TEST_F( TestVirtualVolume, Mount_ToRoot )
{
    auto vv = std::get< VirtualVolume >(Storage::OpenVirtualVolume() );
    ASSERT_TRUE( vv );
    auto pv = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "foo" ) );
    ASSERT_TRUE( pv );
    EXPECT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv, "/", "/", "mount" ) ) );
}


TEST_F( TestVirtualVolume, Mount_ToSamePoint )
{
    auto vv = std::get< VirtualVolume >( Storage::OpenVirtualVolume() );
    ASSERT_TRUE( vv );
    
    auto pv1 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "foo" ) );
    ASSERT_TRUE( pv1 );
    
    auto pv2 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "boo" ) );
    ASSERT_TRUE( pv2 );

    ASSERT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv1, "/", "/", "mount" ) ) );
    EXPECT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv1, "/", "/mount", "mount1" ) ) );
    EXPECT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv2, "/", "/mount", "mount1" ) ) );
}


TEST_F( TestVirtualVolume, Unmount_DependentMounts )
{
    auto vv = std::get< VirtualVolume >( Storage::OpenVirtualVolume( ) );
    ASSERT_TRUE( vv );

    auto pv1 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "foo" ) );
    ASSERT_TRUE( pv1 );

    auto pv2 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "boo" ) );
    ASSERT_TRUE( pv2 );

    auto[ ret, mount ] = vv.Mount( pv1, "/", "/", "mount" );
    ASSERT_EQ( RetCode::Ok, ret );

    ASSERT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv1, "/", "/mount", "mount1" ) ) );
    ASSERT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv2, "/", "/mount", "mount1" ) ) );

    EXPECT_EQ( RetCode::HasDependentMounts, std::get< RetCode >( mount.Close( ) ) );
}

TEST_F( TestVirtualVolume, Unmount_DependentMounts_Force )
{
    auto vv = std::get< VirtualVolume >( Storage::OpenVirtualVolume( ) );
    ASSERT_TRUE( vv );

    auto pv1 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "foo" ) );
    ASSERT_TRUE( pv1 );

    auto pv2 = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "boo" ) );
    ASSERT_TRUE( pv2 );

    auto[ ret, mount ] = vv.Mount( pv1, "/", "/", "mount" );
    ASSERT_EQ( RetCode::Ok, ret );

    ASSERT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv1, "/", "/mount", "mount1" ) ) );
    ASSERT_EQ( RetCode::Ok, std::get< RetCode >( vv.Mount( pv2, "/", "/mount", "mount1" ) ) );

    EXPECT_EQ( RetCode::Ok, std::get< RetCode >( mount.Close( true ) ) );
}

TEST_F( TestVirtualVolume, Get )
{
    auto vv = std::get< VirtualVolume >( Storage::OpenVirtualVolume( ) );
    ASSERT_TRUE( vv );

    auto pv = std::get< PhysicalVolume >( Storage::OpenPhysicalVolume( "foo" ) );
    ASSERT_TRUE( pv );

    auto[ ret, mount ] = vv.Mount( pv, "/", "/", "mount" );
    ASSERT_EQ( RetCode::Ok, ret );

    EXPECT_EQ( RetCode::NotFound, std::get< RetCode >( vv.Get( "/mount/foo" ) ) );
}
