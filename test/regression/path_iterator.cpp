#include <gtest/gtest.h>
#include <path_iterator.h>
#include <string>
#include <string_view>


template < typename StringT >
struct path_iterator_test : public ::testing::Test
{
    using test_type = StringT;
    using string_type = std::basic_string< typename test_type::value_type, typename test_type::traits_type >;
    using view_type = std::basic_string_view< typename test_type::value_type, typename test_type::traits_type >;

    static string_type make_string( char * str )
    {
        //auto size = std::char_traits< char >::length( str );
        //string_type string( size, 0 );
        //for ( decltype( size ) i = 0; i < size; ++i ) string[ i ] = str[ i ];
        //return string;

        std::string tmp( str );
        string_type result( tmp.begin(), tmp.end() );
        return result;
    }
};

typedef ::testing::Types<
    std::string,
    std::wstring,
    std::u16string,
    std::u32string,
    std::string_view,
    std::wstring_view,
    std::u16string_view,
    std::u32string_view
> TestingPolicies;


TYPED_TEST_CASE( path_iterator_test, TestingPolicies );


TYPED_TEST( path_iterator_test, 1 )
{
    string_type s = make_string( "foo" );
    test_type string( s );
}
