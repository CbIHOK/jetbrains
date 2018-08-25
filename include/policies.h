#ifndef __JB__POLICIES__H__
#define __JB__POLICIES__H__


#include <string>
#include <boost/variant.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/string.hpp>
#include "windows_policy.h"


namespace jb
{
    struct DefaultPolicies
    {
        using KeyCharT   = char;
        using ValueT     = boost::variant< uint32_t, uint64_t, float, double, std::string >;

        struct VirtualVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;
            static constexpr size_t MountPointLimit = (1 << 10);
        };

        struct PhysicalVolumePolicy
        {
            static constexpr size_t VolumeLimit = 64;               /*!< how many volumes can be opened at the same time */
            static constexpr size_t MountPointLimit = (1 << 10);    /*!< how many times the volume can be mounted in the same time */
            static constexpr size_t MaxTreeDepth = 256;             /*!< maximum depth of tree in physical volume, we need this constant
                                                                        only to avoid heap usage and to allocate memory on stack */

            static constexpr size_t BloomSize = 16 * ( 1 << 20 );   /*!< size of memory block to be used by Bloom filter */

            static constexpr size_t BTreeMinPower = 1024;           /*!< B-tree factor, each B-tree node (except root) MUST contains at least
                                                                        such number of elements */
            static constexpr size_t BTreeDepth = 64;                /*!< maximum depth of BTree, we need this constant only to avoid heap usage
                                                                        and to allocate memory on stack. In reality the limitation by 1024^64
                                                                        subkeys per each key is pretty enough */
            static constexpr size_t BTreeCacheSize = 1024;          /*!< capacity of BTree MRU cache */

            static constexpr size_t ChunkSize = 4096;               /*!< size of chunk in storage file */
            static constexpr size_t ReaderNumber = 32;              /*!< maximum number of parallel readings */
        };

        using OSPolicy = WindowsPolicy;
    };
}

#endif
