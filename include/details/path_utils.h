#ifndef __JB__PATH_UTILS__H__
#define __JB__PATH_UTILS__H__


#include <string>
#include <regex>


namespace jb
{
    namespace details
    {
        template < typename StringT >
        bool is_valid_path( const StringT & path )
        {
            using namespace std::literals;

            using RegexT = std::basic_regex< typename StringT::value_type >;
            using RegexStrT = typename RegexT::string_type;

            static const auto pattern = R"noesc(^(\/[a-zA-Z][\w-]*)+$|^\/$)noesc"s;
            static const RegexT re{ RegexStrT{ pattern.begin(), pattern.end() } };
            return std::regex_match( path.begin(), path.end(), re );
        }


        template < typename StringT >
        bool is_valid_path_segment( const StringT & str )
        {
            using namespace std::literals;

            using RegexT = std::basic_regex< typename StringT::value_type >;
            using RegexStrT = typename RegexT::string_type;

            static const auto pattern = R"noesc(^([a-zA-Z][\w-]*)$)noesc"s;
            static const RegexT re{ RegexStrT{ pattern.begin(), pattern.end() } };
            return std::regex_match( str.begin(), str.end(), re );
        }


        template < typename StringT >
        std::basic_string_view< typename StringT::value_type, typename StringT::traits_type > trim_separators( const StringT & path ) noexcept
        {
            static constexpr StringT::value_type separator = '/';

            std::basic_string_view< typename StringT::value_type, typename StringT::traits_type > view( path );

            while ( view.size() && view.front() == separator ) view.remove_prefix( 1 ); // does not throw cuz view is not empty
            while ( view.size() && view.back() == separator ) view.remove_suffix( 1 );

            return view;
        }
    }
}

#endif
