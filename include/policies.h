#ifndef __JB__POLICIES__H__
#define __JB__POLICIES__H__


#include <variant>
#include <string>
#include <string_view>
#include <functional>


namespace policies
{
    struct DefaultPolicies
    {
        using KeyCharT = char;
        using ValueT = std::variant< uint32_t, uint64_t, float, double, std::string >;
        static constexpr KeyCharT KeySeparator = '/';


        template < typename CharT = KeyCharT >
        struct  KeyPolicyT
        {
            using KeyValueT   = std::basic_string< CharT >;
            using KeyRefT     = std::basic_string_view< CharT >;
            using KeyHashF    = std::hash< KeyRefT >;
            using KeyHashT    = decltype(std::declval< KeyHashF >()(std::declval< KeyRefT >()));
            using KeyHashRefT = KeyHashT;
        };
        using KeyPolicy = KeyPolicyT<>;


        struct BTreePolicy
        {
            static constexpr size_t Power = 8;
        };


        struct StoragePolicy
        {
            using IndexT = size_t;
        };
    };
}

#endif
