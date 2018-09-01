#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


class TestStorage : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicy<>;
    using Storage = ::jb::Storage< Policies >;
    using RetCode = typename Storage::RetCode;
    using VirtualVolume = typename Storage::VirtualVolume;
    using PhysicalVolume = typename Storage::PhysicalVolume;
    using MountPoint = typename Storage::MountPoint;
    using Value = Storage::Value;

public:

    ~TestStorage()
    {
        using namespace std;

        Storage::CloseAll();

        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
    }


    bool is_lesser_priority( const PhysicalVolume & l, const PhysicalVolume & r ) const
    {
        using namespace std;
        
        auto l_impl = l.impl_.lock( );
        auto r_impl = r.impl_.lock( );

        assert( l_impl && r_impl );

        auto lesser = Storage::get_lesser_priority();

        return lesser( l_impl, r_impl );
    }
};



TEST_F( TestStorage, PhysicalVolume_Dummy )
{
    PhysicalVolume v1;
    EXPECT_FALSE( v1 );

    {
        PhysicalVolume v2;
        EXPECT_FALSE( v2 );
        EXPECT_TRUE( v1 == v2 );
        EXPECT_FALSE( v1 != v2 );

        size_t h1 = Storage::variadic_hash( v1 );
        size_t h2 = Storage::variadic_hash( v2 );
        EXPECT_EQ( h1, h2 );
    }
}


TEST_F( TestStorage, PhysicalVolume_Base )
{
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


TEST_F( TestStorage, PhysicalVolume_Limit )
{
    std::set< PhysicalVolume > set;
    std::unordered_set< PhysicalVolume, Storage::Hash< PhysicalVolume > > hash;

    for ( size_t i = 0; i < Policies::PhysicalVolumePolicy::VolumeLimit; ++i )
    {
        auto[ ret, volume ] = Storage::OpenPhysicalVolume( "foo_" + std::to_string( i ) );
        EXPECT_EQ( RetCode::Ok, ret );
        set.insert( volume );
        hash.insert( volume );
    }

    EXPECT_EQ( set.size(), Policies::PhysicalVolumePolicy::VolumeLimit );
    EXPECT_EQ( hash.size(), Policies::PhysicalVolumePolicy::VolumeLimit );

    auto[ ret, volume ] = Storage::OpenPhysicalVolume( "foo" );
    EXPECT_EQ( RetCode::LimitReached, ret );
    EXPECT_FALSE( volume );

    EXPECT_EQ( RetCode::Ok, Storage::CloseAll() );

    for ( auto volume : set ) EXPECT_FALSE( volume );
}


TEST_F( TestStorage, PhysicalVolume_Priorities )
{
    auto[ ret_1, pv_1 ] = Storage::OpenPhysicalVolume( "foo1" );
    ASSERT_EQ( RetCode::Ok, ret_1 );
    ASSERT_TRUE( pv_1 );

    auto[ ret_2, pv_2 ] = Storage::OpenPhysicalVolume( "foo2" );
    ASSERT_EQ( RetCode::Ok, ret_2 );
    ASSERT_TRUE( pv_2 );
    EXPECT_FALSE( is_lesser_priority( pv_2, pv_1 ) );

    auto[ ret_3, pv_3 ] = Storage::OpenPhysicalVolume( "foo3" );
    ASSERT_EQ( RetCode::Ok, ret_3 );
    ASSERT_TRUE( pv_3 );
    EXPECT_FALSE( is_lesser_priority( pv_3, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv_3, pv_2 ) );

    auto[ ret_4, pv_4 ] = Storage::OpenPhysicalVolume( "foo4" );
    ASSERT_EQ( RetCode::Ok, ret_4 );
    ASSERT_TRUE( pv_4 );

    auto[ ret_5, pv_5 ] = Storage::OpenPhysicalVolume( "foo5" );
    ASSERT_EQ( RetCode::Ok, ret_5 );
    ASSERT_TRUE( pv_5 );

    auto[ ret, pv ] = Storage::OpenPhysicalVolume( "foo" );
    ASSERT_EQ( RetCode::Ok, ret );
    ASSERT_TRUE( pv );
    EXPECT_FALSE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeOnTop( );
    EXPECT_TRUE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeOnBottom( );
    EXPECT_FALSE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeBefore( pv_1 );
    EXPECT_TRUE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeAfter( pv_5 );
    EXPECT_FALSE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeBefore( pv_3 );
    EXPECT_FALSE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_5 ) );

    pv.PrioritizeAfter( pv_3 );
    EXPECT_FALSE( is_lesser_priority( pv, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_2 ) );
    EXPECT_FALSE( is_lesser_priority( pv, pv_3 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_4 ) );
    EXPECT_TRUE( is_lesser_priority( pv, pv_5 ) );
}
 