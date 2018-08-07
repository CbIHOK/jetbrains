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

#include <boost/container/static_vector.hpp>

namespace jb
{

    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::VirtualVolume::Impl
    {

        friend typename Pad;
        friend class VirtualVolume;

        //
        // Few aliases
        //
        using KeyCharT = typename Policies::KeyCharT;
        using KeyValueT = typename Policies::KeyValueT;
        using KeyRefT  = typename Policies::KeyPolicy::KeyRefT;
        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        using KeyHashF = typename Policies::KeyPolicy::KeyHashF;
        static constexpr size_t MountPointLimit = Policies::VirtualVolumePolicy::MountPointLimit;
        using MountPoint = typename Storage::MountPoint;
        using MountPointImpl = typename Storage::MountPoint::Impl;
        using MountPointImplP = std::shared_ptr< MountPointImpl >;


        //
        // just a read/write guard
        //
        std::shared_mutex guard_;


        //
        // keeps mount point backtarces. Different scenario imply different searches through mount
        // collection, and we're going to use separate hashed arrays to keep searches O(1). This
        // structure keeps releation between the arrays as iterators and let us easily move from
        // one collection to another. Take into account that hashed arrays guarantee that iterators
        // stays valid after insert()/erase() operations until rehashing routine, and we can simply
        // pre-allocate the arrays with maximum number of buckets to avoid iterator invalidation
        //
        // a forwarding declaration, see below fro details
        //
        struct MountPointBacktrace;
        using MountPointBacktraceP = std::shared_ptr< MountPointBacktrace >;


        //
        // holds unique mount description that takes into account logical path, physical volume, and
        // physical path. Let us to avoid identical mounts with O(1) complexity
        //
        using MountUid = size_t;
        using MountUidCollectionT = std::unordered_map< MountUid, MountPointBacktraceP >;
        MountUidCollectionT uids_;


        //
        // provides O(1) search by mount PIMP pointer, cover dismount use case
        //
        using MountPointImplCollectionT = std::unordered_map < MountPointImplP, MountPointBacktraceP >;
        MountPointImplCollectionT mounts_;


        //
        // provides O(1) search my mount path, cover most of scenario
        //
        using MountedPathCollectionT = std::unordered_multimap< KeyHashT, MountPointBacktraceP >;
        MountedPathCollectionT paths_;


        //
        // just a postponed definition
        //
        struct MountPointBacktrace
        {
            typename MountUidCollectionT::const_iterator uid_;
            typename MountPointImplCollectionT::const_iterator mount_;
            typename MountedPathCollectionT::const_iterator path_;
        };


        /* Check if another mount may cause the arrays rehashing...
        
        ...and therefore invalidation of held iterators

        @return true if mount acceptable
        @throw nothing
        */
        [[nodiscard]]
        auto check_rehash() const noexcept
        {
            auto check = [](auto hash) {
                return hash.size() + 1 <= hash.max_load_factor() * hash.bucket_count();
            };
            return check(uids_) & check(mounts_) & check(paths_);
        }


        /* Checks if given key represents valid path and normalize it

        @param [in] key - key to be normalized

        @return std::tuple<
            bool - true if key represents valid path
            KeyValueT - normalized key
        >

        @throw std::exception if unrecoverable error occuder
        */
        [[nodiscard]]
        static auto normalize_as_path(KeyRefT key)
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


        /* Checks if given key represents leaf name, i.e. does not contain path, and normalize it

        @param [in] key - key to be normalized

        @return std::tuple<
        bool - true if key represents valid leaf name
        KeyValueT - normalized key
        >

        @throw std::exception if unrecoverable error occuder
        */
        [[nodiscard]]
        static auto normalize_as_leaf(KeyRefT key)
        {
            using namespace std::filesystem;

            filesystem::path p{ key };

            if ( !p.has_root_directory() && p.has_filename() )
            {
                return std::make_tuple(true, p.lexically_normal().string<KeyCharT>());
            }
            else
            {
                return std::make_tuple(false, KeyValueT());
            }
        }


        [[nodiscard]]
        static auto get_parent_key(KeyRefT key) noexcept
        {
            using namespace std::filesystem;

            auto last_separator_pos = key.find_last_of( KeyCharT(path::preferred_separator) );
            
            assert( last_separator_pos != KeyRefT::npos );
            assert( last_seperator_pos < std::min)

            return key.substr(0, last_separator_pos);
        }


        auto unmount(MountPoint * mp)
        {
        }


    public:

        Impl() : guard_()
            , uids_(MountPointLimit)
            , mounts_(MountPointLimit)
            , paths_(MountPointLimit)
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

        auto Mount(PhysicalVolume volume, KeyRefT physical_path, KeyRefT logical_path) noexcept
        {
            using namespace std;

            try
            {
                // validate logical path
                auto[logical_path_valid, normalized_logical_path] = normalize_as_path(logical_path);
                if (!logical_path_valid)
                {
                    return std::pair{ RetCode::InvalidLogicalKey, Mount() };
                }

                // validate physical path
                auto[physical_path_valid, normalized_physical_path] = normalize_as_path(physical_path);
                if (!physical_path_valid)
                {
                    return std::pair{ RetCode::InvalidPhysicalKey, Mount() };
                }

                // start mounting
                std::unique_lock<std::shared_mutex> write_lock(guard_);

                // if maximum number of mount reached?
                if (uids_.size() >= MountPointLimit)
                {
                    return std::pair{ RetCode::MountPointLimitReached, Mount() };
                }

                // if equivalent mount exists?
                auto uid = misc::variadic_hash( normalized_logical_path/*, volume*/); // TODO: implement hash<pv>
                if (uids_.find(uid) != uids_.end())
                {
                    return std::pair{ RetCode::AlreadyExists, Mount() };
                }

                // check that iterators won't be invalidated on insertion
                if ( ! check_rehash() )
                {
                    assert(false); // that's unexpected
                    return std::pair{ RetCode::UnknownError, Mount() };
                }

                // request mount from physical volume
                if ( auto [ ret, mount ] = volume->get_mount( physical_path ); !mount )
                {
                    return std::pair{ ret, Mount() };
                }

                // TODO consider using custom allocattion
                auto backtrace = std::make_shared< MountPointBacktrace >( 
                    uids_.end(), mounts_.end(), paths_.end()
                );

                try
                {
                    backtrace->uid_ = uids_.insert( { uid, backtrace } ).first;
                    backtrace->mount_ = mounts_.insert( { mount, backtrace } ).first;
                    backtrace->path_ = paths_.insert( { KeyHashF()(logical_path), backtrace } );
                }
                catch (...) // if something went wrong - rollback all
                {
                    auto rollback = []( auto container, auto it ) {
                        if (it != container.end()) container.erase( it );
                    };

                    rollback( uids_, backtrace->uid_ );
                    rollback( mounts_, backtrace->mount_ );
                    rollback( paths_, backtrace->path_ );

                    return std::pair{ RetCode::UnknownError, Mount() };
                }

                return std::pair{ RetCode::Ok, std::move( MountPoint( mount ) ) };
            }
            catch (...)
            {
            }

            return std::pair{ RetCode::UnknownError, Mount() };
        }
    };
}


#include "mount_point.h"


#endif
