#include <gtest/gtest.h>
#include <key.h>
#include <policies.h>
#include <storage.h>


using namespace std;


TEST( Key, Dummy )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

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

    key = move( Key{ "/" } );
    EXPECT_TRUE( key == Key{} );
}


TEST( Key, Construction )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

    Key key{ "/foo/boo" };
    EXPECT_TRUE( key.is_valid( ) );
    EXPECT_TRUE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = move( Key{ "boo" } );
    EXPECT_TRUE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_TRUE( key.is_leaf( ) );

    key = Key{ "" };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = move( Key{ "foo/" } );
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = Key{ ":/boo/" };
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );

    key = move( Key{ "c:/boo/" } );
    EXPECT_FALSE( key.is_valid( ) );
    EXPECT_FALSE( key.is_path( ) );
    EXPECT_FALSE( key.is_leaf( ) );
}


TEST( Key, Compare )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPad >;
    using Hash = ::jb::Hash< ::jb::DefaultPolicies, ::jb::DefaultPad, Key >;

    EXPECT_EQ( Key{ "boo"sv }, Key{ "boo" } );
    EXPECT_NE( Key{ "boo1" }, Key{ "boo2"sv } );
    EXPECT_LT( Key{ "boo1" }, Key{ "boo2" } );
    EXPECT_LE( Key{ "boo1" }, Key{ "boo2" } );
    EXPECT_GE( Key{ "boo2" }, Key{ "boo1" } );
    EXPECT_GT( Key{ "boo2" }, Key{ "boo1" } );
    EXPECT_EQ( Hash{}( Key{ "boo" } ), Hash{}( Key{ "boo" } ) );
    EXPECT_NE( Hash{}( Key{ "boo" } ), Hash{}( Key{ "foo" } ) );
}


TEST( Key, SplitAtHead )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

    {
        auto[ ret, super_key, sub_key ] = Key{}.split_at_head( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/" }.split_at_head( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/foo" }.split_at_head( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo" }, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/foo/boo" }.split_at_head( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo" }, super_key );
        EXPECT_EQ( Key{ "/boo" }, sub_key );
    }
}


TEST( Key, SplitAtTile )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

    {
        auto[ ret, super_key, sub_key ] = Key{}.split_at_tile( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/" }.split_at_tile( );
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/foo" }.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{ "/foo" }, sub_key );
    }

    {
        auto[ ret, super_key, sub_key ] = Key{ "/foo/boo" }.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo" }, super_key );
        EXPECT_EQ( Key{ "/boo" }, sub_key );
    }
}

TEST( Key, IsSubkey )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

    EXPECT_FALSE( Key{}.is_subkey( Key{} ) );
    EXPECT_FALSE( Key{ "/foo" }.is_subkey( Key{} ) );
    EXPECT_FALSE( Key{ "" }.is_subkey( Key{ "/foo" } ) );
    EXPECT_FALSE( Key{ "/boo" }.is_subkey( Key{ "boo" } ) );
    EXPECT_FALSE( Key{ "boo" }.is_subkey( Key{ "/boo" } ) );
    EXPECT_FALSE( Key{ "/foo" }.is_subkey( Key{ "/boo" } ) );
    EXPECT_FALSE( Key{ "/foo" }.is_subkey( Key{ "/boo/foo" } ) );
    EXPECT_TRUE( Key{ "/foo/boo" }.is_subkey( Key{ "/foo" } ) );
}

TEST( Key, IsSuperkey )
{
    using Key = ::jb::Key< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;

    EXPECT_FALSE( Key{}.is_superkey( Key{} ) );
    EXPECT_FALSE( Key{ "/foo" }.is_superkey( Key{} ) );
    EXPECT_FALSE( Key{ "" }.is_superkey( Key{ "/foo" } ) );
    EXPECT_FALSE( Key{ "/boo" }.is_superkey( Key{ "boo" } ) );
    EXPECT_FALSE( Key{ "boo" }.is_superkey( Key{ "/boo" } ) );
    EXPECT_FALSE( Key{ "/foo" }.is_superkey( Key{ "/boo" } ) );
    EXPECT_FALSE( Key{ "/foo" }.is_superkey( Key{ "/boo/foo" } ) );
    EXPECT_TRUE( Key{ "/foo" }.is_superkey( Key{ "/foo/boo" } ) );
}
