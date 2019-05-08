#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <string>
#include <string_view>
#include <regex>
#include <assert.h>

namespace jb
{
    template < typename Policies > using KeyValue = std::basic_string< typename Policies::KeyCharT >;
    template < typename Policies > using KeyView = std::basic_string_view< typename Policies::KeyCharT >;

    template < typename Policies >
    constexpr typename Policies::KeyCharT separator() noexcept
    {
        static typename Policies::KeyCharT separator_ = '/';
        return separator_;
    }

    template < typename Policies >
    constexpr KeyView< Policies > root() noexcept
    {
        static constexpr KeyCharT root_ = { separator(), 0 };
        return KeyView< Policies >{ root_ };
    }

    template < typename StringT >
    bool is_valid_key( const StringT & str )
    {
        using RegexT = std::basic_regex< typename StringT::value_type >;
        using RegexStrT = typename RegexT::string_type;

        auto pattern = R"noesc(^(\/[a-zA-Z][\w-]*)+$|^\/$)noesc"s;
        static const RegexT re{ RegexStrT{ pattern.begin(), pattern.end() } };
        return std::regex_match( str.begin(), std.end(), re );
    }

    template < typename StringT >
    bool is_valid_key_segment( const StringT & str )
    {
        using RegexT = std::basic_regex< typename StringT::value_type >;
        using RegexStrT = typename RegexT::string_type;

        auto pattern = R"noesc(^([a-zA-Z][\w-]*)$)noesc"s;
        static const RegexT re{ RegexStrT{ pattern.begin(), pattern.end() } };
        return std::regex_match( str.begin(), str.end(), re );
    }
}

#endif