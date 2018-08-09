#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <variadic_hash.h>


TEST( VirtualVolume, Dummy )
{
    using Storage = ::jb::Storage<>;
    using VirtualVolume = Storage::VirtualVolume;

    VirtualVolume v1;
    EXPECT_FALSE( v1 );

    {
        VirtualVolume v2;
        EXPECT_FALSE( v2 );
        EXPECT_TRUE( v1 == v2 );
        EXPECT_FALSE( v1 != v2 );

        size_t h1 = ::jb::misc::variadic_hash< ::jb::DefaultPolicies, ::jb::DefaultPad >( v1 );
        size_t h2 = ::jb::misc::variadic_hash< ::jb::DefaultPolicies, ::jb::DefaultPad >( v2 );
        EXPECT_EQ( h1, h2 );
    }
}


TEST( VirtualVolume, Base )
{
    using namespace jb;

    using Storage = ::jb::Storage<>;
    using VirtualVolume = Storage::VirtualVolume;

    auto [ ret, v ] = Storage::OpenVirtualVolume( );
    EXPECT_EQ( RetCode::Ok, ret );
    EXPECT_TRUE( v );
        
    EXPECT_FALSE( v == VirtualVolume{} );
    EXPECT_TRUE( v != VirtualVolume{} );

    VirtualVolume copied{ v };
    EXPECT_TRUE( copied );
    EXPECT_TRUE( v );
    EXPECT_EQ( v, copied );

    copied = v;
    EXPECT_TRUE( copied );
    EXPECT_TRUE( v );
    EXPECT_EQ( v, copied );

    VirtualVolume moved{ std::move( v ) };
    EXPECT_FALSE( v );
    EXPECT_TRUE( moved );
    EXPECT_EQ( copied, moved );

    v = std::move( moved );
    EXPECT_TRUE( v );
    EXPECT_FALSE( moved );
    EXPECT_EQ( copied, v );

    EXPECT_EQ( RetCode::Ok, copied.Close( ) );
    EXPECT_EQ( RetCode::InvalidHandle, v.Close( ) );
    EXPECT_EQ( v, copied );
    EXPECT_FALSE( v );
    EXPECT_FALSE( copied );
}


TEST( VirtualVolume, Limit )
{
    using Storage = ::jb::Storage<>;
    using VirtualVolume = Storage::VirtualVolume;

    using namespace jb;

    std::set< VirtualVolume, std::less< VirtualVolume > > set;
    std::unordered_set< VirtualVolume, Hash< DefaultPolicies, DefaultPad, VirtualVolume > > hash;

    for ( size_t i = 0; i < DefaultPolicies::VirtualVolumePolicy::VolumeLimit; ++i )
    {
        auto[ ret, volume ] = Storage::OpenVirtualVolume( );
        EXPECT_EQ( RetCode::Ok, ret );
        set.insert( volume );
        hash.insert( volume );
    }

    EXPECT_EQ( set.size( ), DefaultPolicies::VirtualVolumePolicy::VolumeLimit );
    EXPECT_EQ( hash.size( ), DefaultPolicies::VirtualVolumePolicy::VolumeLimit );

    auto[ ret, volume ] = Storage::OpenVirtualVolume( );
    EXPECT_EQ( RetCode::LimitReached, ret );
    EXPECT_FALSE( volume );

    EXPECT_EQ( RetCode::Ok, Storage::CloseAll() );

    for ( auto volume : set ) EXPECT_FALSE( volume );
}

TEST( PhysicalVolume, Dummy )
{
    using Storage = ::jb::Storage<>;
    using PhysicalVolume = Storage::PhysicalVolume;

    PhysicalVolume v1;
    EXPECT_FALSE( v1 );

    {
        PhysicalVolume v2;
        EXPECT_FALSE( v2 );
        EXPECT_TRUE( v1 == v2 );
        EXPECT_FALSE( v1 != v2 );

        size_t h1 = ::jb::misc::variadic_hash< ::jb::DefaultPolicies, ::jb::DefaultPad >( v1 );
        size_t h2 = ::jb::misc::variadic_hash< ::jb::DefaultPolicies, ::jb::DefaultPad >( v2 );
        EXPECT_EQ( h1, h2 );
    }
}

TEST( PhysicalVolume, Base )
{
    using namespace jb;

    using Storage = ::jb::Storage<>;
    using PhysicalVolume = Storage::PhysicalVolume;

    auto[ ret, v ] = Storage::OpenPhysicalVolume( "foo" );
    EXPECT_EQ( RetCode::Ok, ret );
    EXPECT_TRUE( v );

    EXPECT_FALSE( v == PhysicalVolume{} );
    EXPECT_TRUE( v != PhysicalVolume{} );

    PhysicalVolume copied{ v };
    EXPECT_TRUE( copied );
    EXPECT_TRUE( v );
    EXPECT_EQ( v, copied );

    copied = v;
    EXPECT_TRUE( copied );
    EXPECT_TRUE( v );
    EXPECT_EQ( v, copied );

    PhysicalVolume moved{ std::move( v ) };
    EXPECT_FALSE( v );
    EXPECT_TRUE( moved );
    EXPECT_EQ( copied, moved );

    v = std::move( moved );
    EXPECT_TRUE( v );
    EXPECT_FALSE( moved );
    EXPECT_EQ( copied, v );

    EXPECT_EQ( RetCode::Ok, copied.Close( ) );
    EXPECT_EQ( RetCode::InvalidHandle, v.Close( ) );
    EXPECT_EQ( v, copied );
    EXPECT_FALSE( v );
    EXPECT_FALSE( copied );
}
