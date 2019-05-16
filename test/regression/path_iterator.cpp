#include <gtest/gtest.h>
#include <path_iterator.h>
#include <string>
#include <string_view>


using namespace ::jb::detail;


template < typename StringT >
struct path_iterator_test : public ::testing::Test
{
    using test_type = StringT;
    using string_type = std::basic_string< typename test_type::value_type, typename test_type::traits_type >;
    using view_type = std::basic_string_view< typename test_type::value_type, typename test_type::traits_type >;

    static string_type make_string( char * str )
    {
        std::string tmp( str );
        string_type result( tmp.begin(), tmp.end() );
        return result;
    }
};

typedef ::testing::Types<
    std::string,
    std::wstring,
    std::string_view,
    std::wstring_view
> TestingPolicies;


TYPED_TEST_CASE( path_iterator_test, TestingPolicies );


TYPED_TEST( path_iterator_test, invalid_iterator )
{
    path_iterator< test_type > i;
    EXPECT_NO_THROW( EXPECT_FALSE( i.is_valid() ) );

    EXPECT_NO_THROW( path_iterator< test_type > j( i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );

    path_iterator< test_type > j;
    EXPECT_NO_THROW( j = i );
    EXPECT_NO_THROW( EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );

#ifdef _DEBUG
    EXPECT_DEATH( *i, "" );
#else
    EXPECT_NO_THROW( *i );
#endif

    EXPECT_NO_THROW( ++i; EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );
    EXPECT_NO_THROW( i++; EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );
    EXPECT_NO_THROW( --i; EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );
    EXPECT_NO_THROW( i--; EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );
}


TYPED_TEST( path_iterator_test, empty_path )
{
    auto tmp = make_string( "" );
    test_type path{ tmp };

#ifdef _DEBUG
    EXPECT_DEATH( path_begin( path ), "" );
    EXPECT_DEATH( path_end( path ), "" );
#else
    EXPECT_NO_THROW( EXPECT_EQ( path_iterator< test_type >(), path_begin( path ) ) );
    EXPECT_NO_THROW( EXPECT_EQ( path_iterator< test_type >(), path_end( path ) ) );
#endif
}


TYPED_TEST( path_iterator_test, invalid_path )
{
    auto tmp = make_string( "12345" );
    test_type path{ tmp };

#ifdef _DEBUG
    EXPECT_DEATH( path_begin( path ), "" );
    EXPECT_DEATH( path_end( path ), "" );
#else
    EXPECT_NO_THROW( EXPECT_EQ( path_iterator< test_type >(), path_begin( path ) ) );
    EXPECT_NO_THROW( EXPECT_EQ( path_iterator< test_type >(), path_end( path ) ) );
#endif
}


TYPED_TEST( path_iterator_test, valid_path )
{
    auto tmp = make_string( "/aaa/bbb/ccc/ddd/eee" );
    test_type path{ tmp };

    EXPECT_NO_THROW( auto head = path_begin( path ); EXPECT_TRUE( head.is_valid() ); EXPECT_NE( head, path_iterator< test_type >() ) );
    EXPECT_NO_THROW( auto tile = path_end( path ); EXPECT_TRUE( tile.is_valid() ); EXPECT_NE( tile, path_iterator< test_type >() ) );

    auto head = path_begin( path );
    auto tile = path_end( path );

    EXPECT_NO_THROW( auto i = head; --i; EXPECT_NE( head, i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, path_iterator< test_type >() ) );
    EXPECT_NO_THROW( auto i = tile; ++i; EXPECT_NE( tile, i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, path_iterator< test_type >() ) );

    EXPECT_NO_THROW( auto diff = tile - head++; EXPECT_EQ( make_string( "/aaa/bbb/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - head; EXPECT_EQ( make_string( "/bbb/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - ++head; EXPECT_EQ( make_string( "/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile-- - head; EXPECT_EQ( make_string( "/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - head; EXPECT_EQ( make_string( "/ccc/ddd" ), diff ) );
    EXPECT_NO_THROW( auto diff = --tile - head; EXPECT_EQ( make_string( "/ccc" ), diff ) );
}
