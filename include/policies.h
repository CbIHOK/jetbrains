#ifndef __JB__POLICIES__H__
#define __JB__POLICIES__H__


#include <variant>
#include <string>
#include <functional>
#include <filesystem>
#include "windows_policy.h"


namespace jb
{
    struct DefaultPolicies
    {
        using KeyCharT   = char;
        using ValueT     = std::variant< uint32_t, uint64_t, float, double, std::string >;

        struct VirtualVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;
            static constexpr size_t MountPointLimit = (1 << 10);
        };

        struct PhysicalVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;
            static constexpr size_t MountPointLimit = (1 << 10);
            static constexpr size_t ReaderNumber = 16;

            static constexpr size_t MaxTreeDepth = 32;
            static constexpr size_t BloomSize = 16 * ( 1 << 20 ); // 16 Mb

            static constexpr size_t BTreeMinPower = 1024;
            static constexpr size_t BTreeCacheSize = 1024;

            static constexpr size_t ChunkSize = 64 * (1 << 10); // 64K
        };

        using OSPolicy = WindowsPolicy;
    };
}

#endif
