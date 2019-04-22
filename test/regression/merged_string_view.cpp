#include <merged_string_view.h>
#include <gtest/gtest.h>
#include <string>

template < typename CharT >
class merged_string_view_test : public ::testing::Test
{
public:

    using char_t = CharT;
    using string = std::basic_string< CharT >;
    using view = std::basic_string_view< CharT >;
    using merged_view = jb::detail::merged_string_view< CharT >;

    static string make_string( char * str )
    {
        std::string tmp{ str };
        return string( tmp.begin(), tmp.end() );
    }
};

typedef ::testing::Types<
    char,
    wchar_t,
    char16_t,
    char32_t
> TestingPolicies;

TYPED_TEST_CASE( merged_string_view_test, TestingPolicies );


TYPED_TEST( merged_string_view_test, access_by_index )
{

#ifndef _DEBUG
    {
        constexpr merged_view v{};
        EXPECT_NO_THROW( v[ 0 ] );
    }
#endif

    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str[ i ], v[ i ] ) );
        }
#ifndef _DEBUG
        EXPECT_NO_THROW( v[ str.size() ] );
#endif
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str[ i ], v[ i ] ) );
        }
#ifndef _DEBUG
        EXPECT_NO_THROW( v[ str.size() ] );
#endif
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str[ i ], v[ i ] ) );
        }
#ifndef _DEBUG
        EXPECT_NO_THROW( v[ str.size() ] );
#endif
    }
}