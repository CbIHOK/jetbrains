#ifndef __JB__POLICIES__H__
#define __JB__POLICIES__H__


#include <variant>
#include <string>
#include <functional>
#include <filesystem>


namespace jb
{
    struct DefaultPolicies
    {
        using KeyCharT   = char;
        using ValueT     = std::variant< uint32_t, uint64_t, float, double, std::string >;
        using TimestampT = std::filesystem::file_time_type;


        struct VirtualVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;
            static constexpr size_t MountPointLimit = (1 << 10);
        };

        struct PhysicalVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;
            static constexpr size_t MountPointLimit = (1 << 10);

            static constexpr size_t BloomSize = 16 * ( 1 << 20 ); // 16 Mb
            static constexpr size_t BloomFnCount = 16;
            static constexpr size_t BloomPrecision = 4096;

            static constexpr size_t BTreePower = 1024;
        };
    };
}

#endif
