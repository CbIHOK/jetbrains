#ifndef __JB__POLICIES__H__
#define __JB__POLICIES__H__


#include <variant>
#include <string>
#include <string_view>
#include <functional>


namespace jb
{
    struct DefaultPolicies
    {
        using KeyCharT = char;
        using ValueT = std::variant< uint32_t, uint64_t, float, double, std::string >;


        template < typename CharT = KeyCharT >
        struct  KeyPolicyT
        {
            using KeyValueT   = std::basic_string< CharT >;
            using KeyRefT     = std::basic_string_view< CharT >;
            using KeyHashF    = std::hash< KeyRefT >;
            using KeyHashT    = decltype(std::declval< KeyHashF >()(std::declval< KeyRefT >()));
            using KeyHashRefT = KeyHashT&&;
            
            static constexpr KeyCharT Alphabet[] = "[a-zA-Z0-9_-]";
            static constexpr KeyCharT Separator = '/';
        };
        using KeyPolicy = KeyPolicyT<>;


        struct VirtualVolumePolicy
        {
            static constexpr size_t MountPointLimit = (1 << 10);
        };

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
