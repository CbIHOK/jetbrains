#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


using namespace std;


class TestKey : public ::testing::Test
{
protected:

    using Storage = ::jb::Storage< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;
    using Key = typename Storage::Key;
    using KeyValue = typename Storage::KeyValue;
};


TEST_F( TestKey, Dummy )
{
    Key key;
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );
    EXPECT_TRUE( key == Key{} );
    EXPECT_FALSE( key != Key{} );
    EXPECT_FALSE( key < Key{} );
    EXPECT_TRUE( key <= Key{} );
    EXPECT_FALSE( key > Key{} );
    EXPECT_TRUE( key >= Key{} );

    EXPECT_EQ( "", ( KeyValue )key );
}


TEST_F( TestKey, Construction )
{
    Key key{ "/foo/boo"s };
    EXPECT_TRUE( key.is_valid( ) );
    EXPECT_TRUE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ "boo"s };
    EXPECT_TRUE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_TRUE( key.is_leaf( ) );

    key = Key{ ""s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ "foo/"s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ ":/boo/"s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ "c:/boo/"s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ ".."s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ "."s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ "\foo"s };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );
}


TEST_F( TestKey, Compare )
{
    using Hash = ::jb::Hash< ::jb::DefaultPolicies, ::jb::DefaultPad, Key >;

    EXPECT_EQ( Key{ "boo"s }, Key{ "boo"s } );
    EXPECT_NE( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_LT( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_LE( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_GE( Key{ "boo2"s }, Key{ "boo1"s } );
    EXPECT_GT( Key{ "boo2"s }, Key{ "boo1"s } );
    //EXPECT_EQ( Hash{}( Key{ "boo" } ), Hash{}( Key{ "boo" } ) );
    //EXPECT_NE( Hash{}( Key{ "boo" } ), Hash{}( Key{ "foo" } ) );
}


TEST_F( TestKey, SplitAtHead )
{
    {
        Key in{};
        auto[ ret, super_key, sub_key ] = in.split_at_head( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        Key in{ "/"s };
        auto[ ret, super_key, sub_key ] = in.split_at_head( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        Key in{ "/foo"s };
        auto[ ret, super_key, sub_key ] = in.split_at_head( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        Key in{ "/foo/boo"s };
        auto[ ret, super_key, sub_key ] = in.split_at_head( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{ "/boo"s }, sub_key );
    }
}


TEST_F( TestKey, SplitAtTile )
{
    {
        Key in{};
        auto[ ret, super_key, sub_key ] = in.split_at_tile( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        Key in{ "/"s };
        auto[ ret, super_key, sub_key ] = in.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        Key in{ "/foo"s };
        auto[ ret, super_key, sub_key ] = in.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{ "/foo"s }, sub_key );
    }

    {
        Key in{ "/foo/boo"s };
        auto[ ret, super_key, sub_key ] = in.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{ "/boo"s }, sub_key );
    }
}


TEST_F( TestKey, IsSubkey )
{

    EXPECT_FALSE( Key{}.is_subkey( Key{} ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_subkey( Key{} ) );
    EXPECT_FALSE( Key{}.is_subkey( Key{ "/foo"s } ) );
    EXPECT_FALSE( Key{ "/boo"s }.is_subkey( Key{ "boo"s } ) );
    EXPECT_FALSE( Key{ "boo"s }.is_subkey( Key{ "/boo"s } ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_subkey( Key{ "/boo"s } ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_subkey( Key{ "/boo/foo"s } ) );
    EXPECT_TRUE( Key{ "/foo/boo"s }.is_subkey( Key{ "/foo"s } ) );
}


TEST_F( TestKey, IsSuperkey )
{
    EXPECT_FALSE( Key{}.is_superkey( Key{} ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_superkey( Key{} ) );
    EXPECT_FALSE( Key{}.is_superkey( Key{ "/foo"s } ) );
    EXPECT_FALSE( Key{ "/boo"s }.is_superkey( Key{ "boo"s } ) );
    EXPECT_FALSE( Key{ "boo"s }.is_superkey( Key{ "/boo"s } ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_superkey( Key{ "/boo"s } ) );
    EXPECT_FALSE( Key{ "/foo"s }.is_superkey( Key{ "/boo/foo"s } ) );
    EXPECT_TRUE( Key{ "/foo"s }.is_superkey( Key{ "/foo/boo"s } ) );
}
