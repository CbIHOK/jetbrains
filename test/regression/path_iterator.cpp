#include <gtest/gtest.h>
#include <details/path_iterator.h>
#include <string>
#include <string_view>


template < typename StringT >
struct path_iterator_test : public ::testing::Test
{
    using test_type = StringT;
    using string_type = std::basic_string< typename test_type::value_type, typename test_type::traits_type >;
    using view_type = std::basic_string_view< typename test_type::value_type, typename test_type::traits_type >;
    using path_iterator = jb::details::path_iterator< test_type >;

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


using namespace jb::details;


TYPED_TEST( path_iterator_test, invalid_iterator )
{
    path_iterator i;
    EXPECT_NO_THROW( EXPECT_FALSE( i.is_valid() ) );

    EXPECT_NO_THROW( path_iterator j( i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, j ) );

    path_iterator j;
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
    auto tmp_1 = make_string( "/aaa/bbb/ccc/ddd/eee" );
    test_type path_1{ tmp_1 };

    EXPECT_NO_THROW( auto head = path_begin( path_1 ); EXPECT_TRUE( head.is_valid() ); EXPECT_NE( head, path_iterator() ) );
    EXPECT_NO_THROW( auto tile = path_end( path_1 ); EXPECT_TRUE( tile.is_valid() ); EXPECT_NE( tile, path_iterator() ) );

    auto head = path_begin( path_1 );
    auto tile = path_end( path_1 );

    EXPECT_NO_THROW( auto i = head; --i; EXPECT_NE( head, i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, path_iterator() ) );
    EXPECT_NO_THROW( auto i = tile; ++i; EXPECT_NE( tile, i ); EXPECT_FALSE( i.is_valid() ); EXPECT_EQ( i, path_iterator() ) );

    EXPECT_NO_THROW( auto diff = tile - head++; EXPECT_EQ( make_string( "/aaa/bbb/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - head; EXPECT_EQ( make_string( "/bbb/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - ++head; EXPECT_EQ( make_string( "/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile-- - head; EXPECT_EQ( make_string( "/ccc/ddd/eee" ), diff ) );
    EXPECT_NO_THROW( auto diff = tile - head; EXPECT_EQ( make_string( "/ccc/ddd" ), diff ) );
    EXPECT_NO_THROW( auto diff = --tile - head; EXPECT_EQ( make_string( "/ccc" ), diff ) );

#ifdef _DEBUG
    EXPECT_DEATH( auto diff = --tile - ++head, "" );
#else
    EXPECT_NO_THROW( auto diff = --tile - ++head; EXPECT_EQ( make_string( "" ), diff ) );
#endif

    auto tmp_2 = make_string( "/aaa/bbb/ccc/ddd/eee" );
    test_type path_2{ tmp_2 };

#ifdef _DEBUG
    EXPECT_DEATH( auto diff = path_begin( path_1 ) - path_begin( path_2 ), "" );
#else
    EXPECT_NO_THROW( auto diff = path_begin( path_1 ) - path_begin( path_2 ), EXPECT_EQ( make_string( "" ), diff ) );
#endif
}
