#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <exception>


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
    using namespace std;

    {
        KeyValue in;
        std::tuple< Key, Key > res;
        EXPECT_THROW( res = Key{ in }.split_at_head(), std::logic_error );
    }

    {
        KeyValue in = { "/"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_head() );
        EXPECT_EQ( Key{}, get< 0 >( res ) );
        EXPECT_EQ( Key{}, get< 1 >( res ) );
    }

    {
        KeyValue in{ "/foo"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_head() );
        EXPECT_EQ( Key{ "/foo"s }, get< 0 >( res ) );
        EXPECT_EQ( Key{}, get< 1 >( res ) );
    }

    {
        KeyValue in{ "/foo/boo"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_head() );
        EXPECT_EQ( Key{ "/foo"s }, get< 0 >( res ) );
        EXPECT_EQ( Key{ "/boo"s }, get< 1 >( res ) );
    }
}


TEST_F( TestKey, SplitAtTile )
{
    using namespace std;

    {
        KeyValue in{};
        std::tuple< Key, Key > res;
        EXPECT_THROW( res = Key{ in }.split_at_tile(), logic_error );
    }

    {
        KeyValue in = { "/"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_tile() );
        EXPECT_EQ( Key{}, get< 0 >( res ) );
        EXPECT_EQ( Key{}, get< 1 >( res ) );
    }

    {
        KeyValue in{ "/foo"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_tile() );
        EXPECT_EQ( Key{}, get< 0 >( res ) );
        EXPECT_EQ( Key{ "/foo"s }, get< 1 >( res ) );
    }

    {
        KeyValue in{ "/foo/boo"s };
        std::tuple< Key, Key > res;
        EXPECT_NO_THROW( res = Key{ in }.split_at_tile() );
        EXPECT_EQ( Key{ "/foo"s }, get< 0 >( res ) );
        EXPECT_EQ( Key{ "/boo"s }, get< 1 >( res ) );
    }
}


TEST_F( TestKey, IsSubkey )
{
    using namespace std;

    {
        KeyValue in;
        EXPECT_THROW( Key{ in }.is_subkey( Key{ "/foo"s } ), logic_error );
    }
    
    {
        KeyValue in{ "foo"s };
        EXPECT_THROW( Key{ in }.is_subkey( Key{ "/boo"s } ), logic_error );
    }

    {
        std::tuple< bool, Key > res;
        EXPECT_NO_THROW( res = Key::root().is_subkey( Key::root() ) );
        EXPECT_FALSE( get< 0 >( res ) );
    }

    {
        std::tuple< bool, Key > res;
        EXPECT_NO_THROW( res = Key::root().is_subkey( Key{ "/foo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );
    }

    {
        KeyValue in{ "/foo"s };
        EXPECT_THROW( Key{ in }.is_subkey( Key{} ), logic_error );
        EXPECT_THROW( Key{ in }.is_subkey( Key{ "boo"s } ), logic_error );

        std::tuple< bool, Key > res;

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key::root() ) );
        EXPECT_TRUE( get< 0 >( res ) );
        EXPECT_EQ( Key{ "/foo"s }, get< 1 >( res ) );

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/foo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/boo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/foo/boo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/boo/foo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );
    }

    {
        KeyValue in{ "/foo/boo"s };
        std::tuple< bool, Key > res;

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/boo"s } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ in }.is_subkey( Key{ "/foo"s } ) );
        EXPECT_TRUE( get< 0 >( res ) );
        EXPECT_EQ( Key{ "/boo"s }, get< 1 >( res ) );
    }
}


TEST_F( TestKey, IsSuperkey )
{
    using namespace std;

    {
        KeyValue in;
        EXPECT_THROW( Key{ in }.is_superkey( Key{ "/foo"s } ), logic_error );
    }

    {
        KeyValue in{ "foo"s };
        EXPECT_THROW( Key{ in }.is_superkey( Key{ "/boo"s } ), logic_error );
    }

    {
        std::tuple< bool, Key > res;
        EXPECT_NO_THROW( res = Key::root().is_superkey( Key::root() ) );
        EXPECT_FALSE( get< 0 >( res ) );
    }

    {
        KeyValue foo = "/foo"s;

        std::tuple< bool, Key > res;
        EXPECT_NO_THROW( res = Key::root().is_superkey( Key{ foo } ) );
        EXPECT_TRUE( get< 0 >( res ) );
        EXPECT_EQ( Key{ foo }, get< 1 >( res ) );
    }

    {
        KeyValue foo = "/foo"s;
        KeyValue boo = "/boo"s;
        KeyValue foo_boo = "/foo/boo"s;
        KeyValue boo_foo = "/boo/foo"s;

        EXPECT_THROW( Key{ foo }.is_superkey( Key{} ), logic_error );
        EXPECT_THROW( Key{ foo }.is_superkey( Key{ "boo"s } ), logic_error );

        std::tuple< bool, Key > res;

        EXPECT_NO_THROW( res = Key{ foo }.is_superkey( Key::root() ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ foo }.is_superkey( Key{ foo } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ foo }.is_superkey( Key{ boo } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ foo }.is_superkey( Key{ boo_foo } ) );
        EXPECT_FALSE( get< 0 >( res ) );

        EXPECT_NO_THROW( res = Key{ foo }.is_superkey( Key{ foo_boo } ) );
        EXPECT_TRUE( get< 0 >( res ) );
        EXPECT_EQ( Key{ boo }, get< 1 >( res ) );
    }
}
