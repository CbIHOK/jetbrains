#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


using namespace std;


class TestKey : public ::testing::Test
{
protected:

    using Storage = ::jb::Storage< ::jb::DefaultPolicy<> >;
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
    EXPECT_TRUE( Key{ "/"s }.is_valid() );
    EXPECT_TRUE( Key{ "/"s }.is_path() );
    EXPECT_FALSE( Key{ "/"s }.is_leaf() );

    EXPECT_FALSE( Key{ "//"s }.is_valid() );
    EXPECT_FALSE( Key{ "//"s }.is_path() );
    EXPECT_FALSE( Key{ "//"s }.is_leaf() );

    EXPECT_FALSE( Key{ "//foo"s }.is_valid() );
    EXPECT_FALSE( Key{ "//foo"s }.is_path() );
    EXPECT_FALSE( Key{ "//foo"s }.is_leaf() );

    EXPECT_FALSE( Key{ "/boo//foo"s }.is_valid() );
    EXPECT_FALSE( Key{ "/boo//foo"s }.is_path() );
    EXPECT_FALSE( Key{ "/boo//foo"s }.is_leaf() );

    EXPECT_TRUE( Key{ "boo"s }.is_valid() );
    EXPECT_FALSE( Key{ "boo"s }.is_path() );
    EXPECT_TRUE( Key{ "boo"s }.is_leaf() );

    EXPECT_FALSE( Key{ "bo{o"s }.is_valid() );
    EXPECT_FALSE( Key{ "bo{o"s }.is_path() );
    EXPECT_FALSE( Key{ "bo{o"s }.is_leaf() );

    EXPECT_FALSE( Key{ "boo/"s }.is_valid() );
    EXPECT_FALSE( Key{ "boo/"s }.is_path() );
    EXPECT_FALSE( Key{ "boo/"s }.is_leaf() );

    EXPECT_FALSE( Key{ "/boo/"s }.is_valid() );
    EXPECT_FALSE( Key{ "/boo/"s }.is_path() );
    EXPECT_FALSE( Key{ "/boo/"s }.is_leaf() );

    EXPECT_FALSE( Key{ "-boo"s }.is_valid() );
    EXPECT_FALSE( Key{ "-boo"s }.is_path() );
    EXPECT_FALSE( Key{ "-boo"s }.is_leaf() );

    EXPECT_FALSE( Key{ "_boo"s }.is_valid() );
    EXPECT_FALSE( Key{ "_boo"s }.is_path() );
    EXPECT_FALSE( Key{ "_boo"s }.is_leaf() );

    EXPECT_FALSE( Key{ "1_boo"s }.is_valid() );
    EXPECT_FALSE( Key{ "1_boo"s }.is_path() );
    EXPECT_FALSE( Key{ "1_boo"s }.is_leaf() );

    EXPECT_TRUE( Key{ "foo-1_boo"s }.is_valid() );
    EXPECT_FALSE( Key{ "foo-1_boo"s }.is_path() );
    EXPECT_TRUE( Key{ "foo-1_boo"s }.is_leaf() );

    EXPECT_TRUE( Key{ "/foo-1_boo"s }.is_valid() );
    EXPECT_TRUE( Key{ "/foo-1_boo"s }.is_path() );
    EXPECT_FALSE( Key{ "/foo-1_boo"s }.is_leaf() );
}


TEST_F( TestKey, Compare )
{
    using Hash = Storage::Hash< Key >;

    EXPECT_EQ( Key{ "boo"s }, Key{ "boo"s } );
    EXPECT_NE( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_LT( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_LE( Key{ "boo1"s }, Key{ "boo2"s } );
    EXPECT_GE( Key{ "boo2"s }, Key{ "boo1"s } );
    EXPECT_GT( Key{ "boo2"s }, Key{ "boo1"s } );
    //EXPECT_EQ( Hash{}( Key{ "boo"s } ), Hash{}( Key{ "boo"s } ) );
    //EXPECT_NE( Hash{}( Key{ "boo"s } ), Hash{}( Key{ "foo"s } ) );
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
        KeyValue in = { "/"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_head();
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        KeyValue in{ "/foo"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_head();
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        KeyValue in{ "/foo/boo"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_head();
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{ "/boo"s }, sub_key );
    }
}


TEST_F( TestKey, SplitAtTile )
{
    {
        KeyValue in{};
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_tile();
        EXPECT_FALSE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        KeyValue in{ "/"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{}, sub_key );
    }

    {
        KeyValue in{ "/foo"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{}, super_key );
        EXPECT_EQ( Key{ "/foo"s }, sub_key );
    }

    {
        KeyValue in{ "/foo/boo"s };
        auto[ ret, super_key, sub_key ] = Key{ in }.split_at_tile( );
        EXPECT_TRUE( ret );
        EXPECT_EQ( Key{ "/foo"s }, super_key );
        EXPECT_EQ( Key{ "/boo"s }, sub_key );
    }
}


TEST_F( TestKey, IsSubkey )
{
    EXPECT_FALSE( std::get< bool >( Key{}.is_subkey( Key{} ) ) );

    EXPECT_TRUE( std::get< bool >( Key{ "/foo"s }.is_subkey( Key{} ) ) );
    EXPECT_EQ( Key{ "/foo"s }, std::get< Key >( Key{ "/foo"s }.is_subkey( Key{} ) ) );

    EXPECT_TRUE( std::get< bool >( Key{ "/foo"s }.is_subkey( Key::root() ) ) );
    EXPECT_EQ( Key{ "/foo"s }, std::get< Key >( Key{ "/foo"s }.is_subkey( Key::root() ) ) );

    EXPECT_FALSE( std::get< bool >( Key{}.is_subkey( Key{ "/foo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/boo"s }.is_subkey( Key{ "boo"s } ) ) );

    EXPECT_FALSE( std::get< bool >( Key{ "/boo"s }.is_subkey( Key{ "boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "boo"s }.is_subkey( Key{ "/boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/foo"s }.is_subkey( Key{ "/boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/foo"s }.is_subkey( Key{ "/boo/foo"s } ) ) );

    EXPECT_TRUE( std::get< bool >( Key{ "/foo/boo"s }.is_subkey( Key{ "/foo"s } ) ) );
    EXPECT_EQ( Key{ "/boo"s }, std::get< Key >( Key{ "/foo/boo"s }.is_subkey( Key{ "/foo"s } ) ) );
}


TEST_F( TestKey, IsSuperkey )
{
    EXPECT_FALSE( std::get< bool >( Key{}.is_superkey( Key{} ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/foo"s }.is_superkey( Key{} ) ) );
    
    EXPECT_TRUE( std::get< bool >( Key{}.is_superkey( Key{ "/foo"s } ) ) );
    EXPECT_EQ( Key{ "/foo"s }, std::get< Key >( Key{}.is_superkey( Key{ "/foo"s } ) ) );

    EXPECT_FALSE( std::get< bool >( Key{ "/boo"s }.is_superkey( Key{ "boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "boo"s }.is_superkey( Key{ "/boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/foo"s }.is_superkey( Key{ "/boo"s } ) ) );
    EXPECT_FALSE( std::get< bool >( Key{ "/foo"s }.is_superkey( Key{ "/boo/foo"s } ) ) );
    
    EXPECT_TRUE( std::get< bool >( Key{ "/foo"s }.is_superkey( Key{ "/foo/boo"s } ) ) );
    EXPECT_EQ( Key{ "/boo"s }, std::get< Key >( Key{ "/foo"s }.is_superkey( Key{ "/foo/boo"s } ) ) );
}
