#include <merged_string_view.h>
#include <rare_write_frequent_read_mutex.h>
#include <gtest/gtest.h>
#include <string>
#include <exception>


template < typename CharT >
struct merged_string_view_test : public ::testing::Test
{
    using char_t = CharT;
    using string = std::basic_string< CharT >;
    using view = std::basic_string_view< CharT >;
    using merged_view = jb::detail::merged_string_view< CharT >;
    using iterator = typename merged_view::iterator;
    using reverse_iterator = typename merged_view::reverse_iterator;

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


TYPED_TEST( merged_string_view_test, at )
{

    {
        constexpr merged_view v{};
        EXPECT_THROW( v.at( 0 ), std::out_of_range );
    }

    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str.at( i ), v.at( i ) ) );
        }
        EXPECT_THROW( v.at( str.size() ), std::out_of_range );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str.at( i ), v.at( i ) ) );
        }
        EXPECT_THROW( v.at( str.size() ), std::out_of_range );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };

        for ( string::size_type i = 0; i < str.size(); ++i )
        {
            EXPECT_NO_THROW( EXPECT_EQ( str.at( i ), v.at( i ) ) );
        }
        EXPECT_THROW( v.at( str.size() ), std::out_of_range );
    }
}

TYPED_TEST( merged_string_view_test, front )
{
    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_EQ( str.front(), v.front() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_EQ( str.front(), v.front() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_EQ( str.front(), v.front() ) );
    }
}


TYPED_TEST( merged_string_view_test, back )
{
    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_EQ( str.back(), v.back() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_EQ( str.back(), v.back() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_EQ( str.back(), v.back() ) );
    }
}


TYPED_TEST( merged_string_view_test, size )
{
    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_EQ( str.size(), v.size() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_EQ( str.size(), v.size() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_EQ( str.size(), v.size() ) );
    }
}


TYPED_TEST( merged_string_view_test, length )
{
    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_EQ( str.length(), v.length() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_EQ( str.length(), v.length() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_EQ( str.length(), v.length() ) );
    }
}


TYPED_TEST( merged_string_view_test, max_size )
{
    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_EQ( view{ str }.max_size(), v.max_size() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_EQ( view{ str }.max_size(), v.max_size() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_EQ( view{ str }.max_size(), v.max_size() ) );
    }
}


TYPED_TEST( merged_string_view_test, empty )
{
    {
        merged_view v{ view{}, view{} };
        EXPECT_NO_THROW( EXPECT_TRUE( v.empty() ) );
    }

    {
        string str = make_string( "abcde" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( EXPECT_FALSE( v.empty() ) );
    }

    {
        string str = make_string( "abcdefgh" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( EXPECT_FALSE( v.empty() ) );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        auto str = str1 + str2;
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_FALSE( v.empty() ) );

    }
}

TYPED_TEST( merged_string_view_test, remove_prefix )
{
    {
        merged_view v{ view{}, view{} };
        EXPECT_NO_THROW( v.remove_prefix( 0 ) );
        EXPECT_TRUE( v.empty() );
    }

    {
        merged_view v{ view{}, view{} };
        EXPECT_NO_THROW( v.remove_prefix( 1 ) );
        EXPECT_TRUE( v.empty() );
    }

    {
        string str = make_string( "abcd" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( v.remove_prefix( 0 ) );
        EXPECT_EQ( 'a', v.front() );
        EXPECT_NO_THROW( v.remove_prefix( 2 ) ); // "cd"
        EXPECT_EQ( 'c', v.front() );
        EXPECT_NO_THROW( v.remove_prefix( 4 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str = make_string( "abcd" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( v.remove_prefix( 0 ) );
        EXPECT_EQ( 'a', v.front() );
        EXPECT_NO_THROW( v.remove_prefix( 2 ) ); // "cd"
        EXPECT_EQ( 'c', v.front() );
        EXPECT_NO_THROW( v.remove_prefix( 2 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_prefix( 3 ) ); // "defgh"
        EXPECT_EQ( 'd', v.front() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_prefix( 4 ) ); // "efgh"
        EXPECT_EQ( 'e', v.front() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_prefix( 5 ) ); // "fgh"
        EXPECT_EQ( 'f', v.front() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_prefix( 8 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_prefix( 9 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }
}


TYPED_TEST( merged_string_view_test, remove_suffix )
{
    {
        merged_view v{ view{}, view{} };
        EXPECT_NO_THROW( v.remove_suffix( 0 ) );
        EXPECT_TRUE( v.empty() );
    }

    {
        merged_view v{ view{}, view{} };
        EXPECT_NO_THROW( v.remove_suffix( 1 ) );
        EXPECT_TRUE( v.empty() );
    }

    {
        string str = make_string( "abcd" );
        merged_view v{ str, view{} };
        EXPECT_NO_THROW( v.remove_suffix( 0 ) );
        EXPECT_EQ( 'd', v.back() );
        EXPECT_NO_THROW( v.remove_suffix( 2 ) ); // "ab"
        EXPECT_EQ( 'b', v.back() );
        EXPECT_NO_THROW( v.remove_suffix( 4 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str = make_string( "abcd" );
        merged_view v{ view{}, str };
        EXPECT_NO_THROW( v.remove_suffix( 0 ) );
        EXPECT_EQ( 'd', v.back() );
        EXPECT_NO_THROW( v.remove_suffix( 2 ) ); // "ab"
        EXPECT_EQ( 'b', v.back() );
        EXPECT_NO_THROW( v.remove_suffix( 2 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_suffix( 3 ) ); // "abcde"
        EXPECT_EQ( 'e', v.back() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_suffix( 4 ) ); // "abcd"
        EXPECT_EQ( 'd', v.back() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_suffix( 5 ) ); // "abc"
        EXPECT_EQ( 'c', v.back() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_suffix( 8 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }

    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( v.remove_suffix( 9 ) ); // <empty>
        EXPECT_TRUE( v.empty() );
    }
}


TYPED_TEST( merged_string_view_test, iterator )
{
    // direct iterator
    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        string str = str1 + str2;

        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_NE( v.begin(), v.end() ) );
        EXPECT_EQ( str.size(), std::distance( v.begin(), v.end() ) );
        EXPECT_EQ( str, string( v.begin(), v.end() ) );

        auto i( v.begin() );
        EXPECT_NO_THROW( EXPECT_EQ( v.begin(), i ) );
        i = i++;
        EXPECT_NO_THROW( EXPECT_EQ( v.begin(), i ) );

        EXPECT_NO_THROW( EXPECT_EQ( str[ 0 ], *i++ ) ); // +1
        EXPECT_NO_THROW( EXPECT_EQ( str[ 1 ], *i ) );
        EXPECT_NO_THROW( EXPECT_EQ( str[ 2 ], *++i ) ); // +2

        ++ ++ ++ ++ ++i; // i = +7
        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *i++ ) ); // +8
        EXPECT_NO_THROW( EXPECT_EQ( i, v.end() ) );

#ifndef _DEBUG
        EXPECT_NO_THROW( *i );
#endif
        
        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *--i ) ); // +7
        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *i-- ) ); // +6
        EXPECT_NO_THROW( EXPECT_EQ( str[ 6 ], *i ) );

        -- -- -- -- -- --i; // +0
        EXPECT_NO_THROW( EXPECT_EQ( str[ 0 ], *i ) ); // +8
        EXPECT_NO_THROW( EXPECT_EQ( i, v.begin() ) );
    }

    // reverse iterator
    {
        string str1 = make_string( "abcd" );
        string str2 = make_string( "efgh" );
        string str = str1 + str2;
        std::reverse( str.begin(), str.end() );

        merged_view v{ str1, str2 };
        EXPECT_NO_THROW( EXPECT_NE( v.rbegin(), v.rend() ) );
        EXPECT_EQ( str.size(), std::distance( v.rbegin(), v.rend() ) );
        EXPECT_EQ( str, string( v.rbegin(), v.rend() ) );

        auto i( v.rbegin() );
        EXPECT_NO_THROW( EXPECT_EQ( v.rbegin(), i ) );
        i = i++;
        EXPECT_NO_THROW( EXPECT_EQ( v.rbegin(), i ) );

        EXPECT_NO_THROW( EXPECT_EQ( str[ 0 ], *i++ ) ); // +1
        EXPECT_NO_THROW( EXPECT_EQ( str[ 1 ], *i ) );
        EXPECT_NO_THROW( EXPECT_EQ( str[ 2 ], *++i ) ); // +2

        ++ ++ ++ ++ ++i; // i = +7
        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *i++ ) ); // +8
        EXPECT_NO_THROW( EXPECT_EQ( i, v.rend() ) );

#ifndef _DEBUG
        EXPECT_NO_THROW( *i );
#endif

        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *--i ) ); // +7
        EXPECT_NO_THROW( EXPECT_EQ( str[ 7 ], *i-- ) ); // +6
        EXPECT_NO_THROW( EXPECT_EQ( str[ 6 ], *i ) );

        -- -- -- -- -- --i; // +0
        EXPECT_NO_THROW( EXPECT_EQ( str[ 0 ], *i ) ); // +8
        EXPECT_NO_THROW( EXPECT_EQ( i, v.rbegin() ) );
    }
}
