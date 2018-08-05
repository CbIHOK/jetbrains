#ifndef __JB__VIRTUAL_VOLUME_IMPL__H__
#define __JB__VIRTUAL_VOLUME_IMPL__H__


#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <filesystem>
#include <tuple>
#include <functional>
#include <type_traits>
#include "physical_volume.h"
#include "variadic_hash.h"

namespace jb
{

    template < typename Policies >
    class Storage< Policies >::VirtualVolume::Impl
    {
        //
        // Few aliases
        //
        using KeyCharT = typename Policies::KeyCharT;
        using KeyValueT = typename Policies::KeyValueT;
        using KeyRefT  = typename Policies::KeyPolicy::KeyRefT;
        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        static constexpr size_t MountPointLimit = Policies::VirtualVolumePolicy::MountPointLimit;
        using MountPoint = typename Storage::MountPoint;
        using MountPointImpl = typename Storage::MountPoint::Impl;


        //
        // just a read/write guardr
        //
        std::shared_mutex guard_;


        //
        // mount uid is a unique identifier regarding logical path of mount, physical volume and
        // path to be mounted. The collection prevents equivalent mounts and provides O(1) search
        // by UID in continuous memory 
        //
        using MountPointUID = size_t;
        using MountPointUidCollectionT = std::unordered_set< MountPointUID >;
        MountPointUidCollectionT uids_;


        //
        // holds mount PIMP's, provides O(1) search by PIMP in continuous memory
        //
        using MountPointImplCollectionT = std::unordered_set < std::shared_ptr< MountPointImpl > >;
        MountPointImplCollectionT impls_;


        //
        // keeps mount -> dependent mount links, let's check if thete is a dependencies on a mount
        // by O(1) search in continuous memory
        //
        using MountPointDependencyCollectionT = std::unordered_multimap< KeyHashT, KeyHashT>;
        MountPointDependencyCollectionT dependencies_;


        //
        // keeps logical path -> { 
        //
        struct MountPointTraces
        {
            typename MountPointUidCollectionT::const_iterator uid_;
            typename MountPointImplCollectionT::const_iterator impl_;
            typename MountPointDependencyCollectionT::const_iterator dependency_;
        };
        std::unordered_map< KeyHashT, MountPointTraces > traces_;

        template < typename T >
        auto will_rehash(T && hash)
        {
            return hash.size() + 1 > hash.max_load_factor() * hash.bucket_count();
        }

        auto normalize_as_path(KeyRefT key)
        {
            using namespace std::filesystem;

            filesystem::path p{ key };

            if ( p.has_root_directory() )
            {
                if (p.has_filename())
                {
                    return std::make_tuple(true, p.lexically_normal().string<KeyCharT>());
                }
                else
                {
                    return std::make_tuple(true, p.lexically_normal().parent_path().string<KeyCharT>());
                }
            }
            else
            {
                return std::make_tuple(false, KeyValueT());
            }
        }

        auto normalize_as_leaf(KeyRefT key)
        {
            using namespace std::filesystem;

            filesystem::path p{ key };

            if ( !p.has_root_directory() && p.has_filename() )
            {
                return std::make_tuple(true, (KeyValueT)p.lexically_normal().string<KeyCharT>());
            }
            else
            {
                return std::make_tuple(false, KeyValueT());
            }
        }

        auto get_parent_key(KeyRefT key)
        {
            using namespace std::filesystem;
            auto last_separator_pos = key.find_last_of( KeyCharT(path::preferred_separator) );
            return key.substr(0, last_separator_pos);
        }

        auto unmount(MountPoint * mp)
        {
            ::std::lock(std::shared_lock(guard_));
        }

    public:

        Impl() 
            : guard_()
            , uids_(MountPointLimit)
            , impls_(MountPointLimit)
            , dependencies_(MountPointLimit)
            , traces_(MountPointLimit)
        {
        }

        Impl(Impl&&) = delete;

        auto Insert(KeyRefT path, KeyRefT subkey, ValueT && value, bool overwrite ) noexcept
        {

        }

        auto Get(KeyRefT key) noexcept
        {

        }

        auto Erase(KeyRefT key, bool force) noexcept
        {

        }

        auto Mount(PhysicalVolume volume, KeyRefT physical_path, KeyRefT logical_path ) noexcept
        {
            using namespace std;

            try
            {
                // validate logical path
                auto[logical_path_valid, normalized_logical_path] = normalize_as_path(logical_path);
                if (!logical_path_valid)
                {
                    return std::make_tuple(Storage::RetCode::InvalidKey, MountPoint());
                }

                // validate physical path
                auto[physical_path_valid, normalized_physical_path] = normalize_as_path(physical_path);
                if (!physical_path_valid)
                {
                    return std::make_tuple(Storage::RetCode::InvalidKey, MountPoint());
                }

                // start mounting
                std::unique_lock<std::shared_mutex> write_lock(guard_);

                // if maximum number of mount reached?
                if (impls_.size() >= MountPointLimit)
                {
                    return std::make_tuple(Storage::RetCode::AlreadyExists, MountPoint());
                }

                // if equivalent mount exists?
                auto uid = misc::variadic_hash(logical_path, physical_path);
                if (uids_.find(uid) != uids_.end())
                {
                    return std::make_tuple(Storage::RetCode::AlreadyExists, MountPoint());
                }

                // check that iterators won't be invalidated on insertion
                if ( will_rehash(uids_) || will_rehash(dependencies_) || will_rehash(impls_) )
                {
                    assert(false);
                    throw runtime_error("");
                }
            }
            catch (...)
            {
            }

            return std::make_tuple(Storage::RetCode::UnknownError, MountPoint());
        }
    };
}


#include "mount_point.h"


#endif
