#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


using namespace std;

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

    void SetUp() override
    {
        using namespace std;

        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
    }


    void TearDown() override
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
    auto[ ret, v ] = Storage::OpenPhysicalVolume( "foo.jb" );
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
        auto[ ret, volume ] = Storage::OpenPhysicalVolume( "foo_" + std::to_string( i ) + ".jb" );
        EXPECT_EQ( RetCode::Ok, ret );
        set.insert( volume );
        hash.insert( volume );
    }

    EXPECT_EQ( set.size(), Policies::PhysicalVolumePolicy::VolumeLimit );
    EXPECT_EQ( hash.size(), Policies::PhysicalVolumePolicy::VolumeLimit );

    auto[ ret, volume ] = Storage::OpenPhysicalVolume( "foo.jb" );
    EXPECT_EQ( RetCode::LimitReached, ret );
    EXPECT_FALSE( volume );

    EXPECT_EQ( RetCode::Ok, Storage::CloseAll() );

    for ( auto volume : set ) EXPECT_FALSE( volume );
}


TEST_F( TestStorage, PhysicalVolume_Priorities )
{
    auto[ ret_1, pv_1 ] = Storage::OpenPhysicalVolume( "foo1.jb" );
    ASSERT_EQ( RetCode::Ok, ret_1 );
    ASSERT_TRUE( pv_1 );

    auto[ ret_2, pv_2 ] = Storage::OpenPhysicalVolume( "foo2.jb" );
    ASSERT_EQ( RetCode::Ok, ret_2 );
    ASSERT_TRUE( pv_2 );
    EXPECT_FALSE( is_lesser_priority( pv_2, pv_1 ) );

    auto[ ret_3, pv_3 ] = Storage::OpenPhysicalVolume( "foo3.jb" );
    ASSERT_EQ( RetCode::Ok, ret_3 );
    ASSERT_TRUE( pv_3 );
    EXPECT_FALSE( is_lesser_priority( pv_3, pv_1 ) );
    EXPECT_FALSE( is_lesser_priority( pv_3, pv_2 ) );

    auto[ ret_4, pv_4 ] = Storage::OpenPhysicalVolume( "foo4.jb" );
    ASSERT_EQ( RetCode::Ok, ret_4 );
    ASSERT_TRUE( pv_4 );

    auto[ ret_5, pv_5 ] = Storage::OpenPhysicalVolume( "foo5.jb" );
    ASSERT_EQ( RetCode::Ok, ret_5 );
    ASSERT_TRUE( pv_5 );

    auto[ ret, pv ] = Storage::OpenPhysicalVolume( "foo.jb" );
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
 

//
// Checks insertion to root
//
TEST_F( TestStorage, Insert_to_Root_Get_Erase )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Insert_to_Root_Get_Erase.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "test" } ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ "test" } ), vv.Get( "/pv/foo" ) );
    EXPECT_EQ( RetCode::Ok, vv.Erase( "/pv/foo" ) );
    EXPECT_EQ( make_tuple( RetCode::NotFound, Value{} ), vv.Get( "/pv/foo" ) );
}


//
// Checks insertion to not root
//
TEST_F( TestStorage, Insert_to_Child_Get_Erase )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Insert_to_Child_Get_Erase.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" } ) );
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv/foo", "boo", Value{ "boo" } ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ "boo" } ), vv.Get( "/pv/foo/boo" ) );
    EXPECT_EQ( RetCode::Ok, vv.Erase( "/pv/foo/boo" ) );
    EXPECT_EQ( make_tuple( RetCode::NotFound, Value{} ), vv.Get( "/pv/foo/boo" ) );
}


//
// Checks insertion of expired node
//
TEST_F( TestStorage, Insert_Expired )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Insert_Expired.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 );

    std::this_thread::sleep_for( std::chrono::seconds( 1 ) );

    EXPECT_EQ( RetCode::AlreadyExpired, vv.Insert( "/pv", "foo", Value{ "foo" }, now ) );
}


//
// Checks getting of expired node
//
TEST_F( TestStorage, Get_Expired )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Get_Expired.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 ) + 1000;

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" }, now ) );

    std::this_thread::sleep_for( std::chrono::seconds( 2 ) );

    EXPECT_EQ( make_tuple( RetCode::NotFound, Value{} ), vv.Get( "/pv/foo" ) );
}


//
// Checks navigation over expired node
//
TEST_F( TestStorage, Navigate_Over_Expired )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Navigate_Over_Expired.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 ) + 1000;

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" }, now ) );
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv/foo", "boo", Value{ "boo" } ) );

    std::this_thread::sleep_for( std::chrono::seconds( 2 ) );

    EXPECT_EQ( make_tuple( RetCode::NotFound, Value{} ), vv.Get( "/pv/foo/boo" ) );
}


//
// Checks inserting of already existing node
//
TEST_F( TestStorage, Insert_Existing )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Insert_Existing.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" } ) );
    EXPECT_EQ( RetCode::AlreadyExists, vv.Insert( "/pv", "foo", Value{ "foo" } ) );
}


//
// Checks overwritting of existing node
//
TEST_F( TestStorage, Overwrite_Existing )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Overwrite_Existing.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" } ) );
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "new_value" }, 0, true ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ "new_value" } ), vv.Get( "/pv/foo" ) );
}


//
// Check that removing a rott node of a physical volume is impossible
//
TEST_F( TestStorage, Erase_Root )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Erase_Root.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::InvalidLogicalPath, vv.Erase( "/pv" ) );
}


//
// Check that removing a node locked due to mounting operation is imposible
//
TEST_F( TestStorage, Erase_Locked )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Erase_Locked.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" } ) );

    auto[ rc3, mp2 ] = vv.Mount( pv, "/", "/pv/foo", "foo" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::PathLocked, vv.Erase( "/pv/foo" ) );

    EXPECT_EQ( RetCode::Ok, mp2.Close() );
    EXPECT_EQ( RetCode::Ok, vv.Erase( "/pv/foo" ) );
}


//
// Checks that removing a node having children is not allowed
//
TEST_F( TestStorage, Erase_WithChildren )
{
    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Erase_WithChildren.jb" );
    ASSERT_EQ( RetCode::Ok, rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, mp ] = vv.Mount( pv, "/", "/", "pv" );
    EXPECT_EQ( RetCode::Ok, rc2 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv", "foo", Value{ "foo" } ) );
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/pv/foo", "boo", Value{ "boo" } ) );

    EXPECT_EQ( RetCode::NotLeaf, vv.Erase( "/pv/foo" ) );
}


//
// Checks that operations on mount points regard physical volume priorities
//
TEST_F( TestStorage, VolumePriority )
{
    auto[ rc1, pv1 ] = Storage::OpenPhysicalVolume( "VolumePriority_1.jb" );
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, pv2 ] = Storage::OpenPhysicalVolume( "VolumePriority_2.jb" );
    ASSERT_EQ( RetCode::Ok, rc2 );

    auto[ rc, pv3 ] = Storage::OpenPhysicalVolume( "VolumePriority_3.jb" );
    ASSERT_EQ( RetCode::Ok, rc2 );

    auto[ rc3, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc3 );

    auto[ rc4, mp1 ] = vv.Mount( pv1, "/", "/", "v1" );
    EXPECT_EQ( RetCode::Ok, rc4 );

    auto[ rc5, mp2 ] = vv.Mount( pv2, "/", "/", "v2" );
    EXPECT_EQ( RetCode::Ok, rc5 );

    auto[ rc6, mp3 ] = vv.Mount( pv3, "/", "/", "v3" );
    EXPECT_EQ( RetCode::Ok, rc6 );

    auto[ rc7, sum1 ] = vv.Mount( pv1, "/", "/v3", "sum" );
    EXPECT_EQ( RetCode::Ok, rc7 );

    auto[ rc8, sum2 ] = vv.Mount( pv2, "/", "/v3", "sum" );
    EXPECT_EQ( RetCode::Ok, rc8 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ 1U } ) );
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v2", "foo", Value{ 2U } ) );

    // expect that get() selects value from pv1
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ 1U } ), vv.Get( "/v3/sum/foo" ) );

    // expect that insert() inserts value to pv1
    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v3/sum", "boo", Value{ "boo" } ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ "boo" } ), vv.Get( "/v1/boo" ) );

    // expect that erase() deletes node from pv1
    EXPECT_EQ( RetCode::Ok, vv.Erase( "/v3/sum/foo" ) );
    EXPECT_EQ( make_tuple( RetCode::NotFound, Value{} ), vv.Get( "/v1/foo" ) );
}


//
// Checks that operations on mount points regard physical volume priorities
//
TEST_F( TestStorage, Types )
{
    auto[ rc1, pv] = Storage::OpenPhysicalVolume( "VolumePriority_1.jb" );
    ASSERT_EQ( RetCode::Ok, rc1 );

    auto[ rc2, vv ] = Storage::OpenVirtualVolume();
    ASSERT_EQ( RetCode::Ok, rc2 );

    auto[ rc3, mp ] = vv.Mount( pv, "/", "/", "v1" );
    EXPECT_EQ( RetCode::Ok, rc3 );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ ( uint32_t )1234 } ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ ( uint32_t )1234 } ), vv.Get( "/v1/foo" ) );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ ( uint64_t )1234 }, 0, true ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ ( uint64_t )1234 } ), vv.Get( "/v1/foo" ) );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ ( float )1234 }, 0, true ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ ( float )1234 } ), vv.Get( "/v1/foo" ) );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ ( double )1234 }, 0, true ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ ( double )1234 } ), vv.Get( "/v1/foo" ) );

    EXPECT_EQ( RetCode::Ok, vv.Insert( "/v1", "foo", Value{ "1234" }, 0, true ) );
    EXPECT_EQ( make_tuple( RetCode::Ok, Value{ "1234" } ), vv.Get( "/v1/foo" ) );
}
