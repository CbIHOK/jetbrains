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
#include "variadic_hash.h"


class TestVirtualVolume;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::VirtualVolumeImpl
    {
        friend typename Pad;
        friend class TestVirtualVolume;

        //
        // Few aliases
        //
        using Storage = ::jb::Storage< Policies, Pad >;
        using PhysicalVolumeImpl = typename Storage::PhysicalVolumeImpl;
        using PhysicalVolumeImplP = std::shared_ptr< PhysicalVolumeImpl >;
        using MountPoint = typename Storage::MountPoint;
        using MountPointImpl = typename Storage::MountPointImpl;
       
        using KeyT = typename Storage::KeyT;
        using ValueT = typename Storage::ValueT;
        using TimestampT = typename Storage::TimestampT;

        static constexpr size_t MountsLimit = Policies::VirtualVolumePolicy::MountPointLimit;


        //
        // just a read/write guard
        //
        std::shared_mutex mounts_guard_;


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
        using MountPointImplCollectionT = std::unordered_map <
            std::shared_ptr< MountPointImpl >,
            MountPointBacktraceP
        >;
        MountPointImplCollectionT mounts_;


        //
        // provides O(1) search my mount path, cover most of scenario
        //
        using MountedPathCollectionT = std::unordered_multimap< KeyT, MountPointBacktraceP >;
        MountedPathCollectionT mounted_paths_;


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
        [[ nodiscard ]]
        auto check_rehash() const noexcept
        {
            auto check = [] ( auto hash ) {
                return hash.size() + 1 <= hash.max_load_factor() * hash.bucket_count();
            };
            return check( uids_ ) & check( mounts_ ) & check( paths_ );
        }


        auto find_nearest_mounted_path( const KeyT & logical_path ) const noexcept
        {
            auto[ res, parent, rest ] = logical_path.split_at_tile( );
            assert( res );

            while ( parent != Key{} )
            {
                if ( mounted_paths_.find( parent ) != mounted_paths_.end( ) )
                {
                    return pair{ parent, rest };
                }
            }

            return pair{ Key{}, logical_path };
        }



        auto unmount( const MountPoint & mp )
        {
        }


    public:

        VirtualVolumeImpl( ) : guard_( )
            , uids_( MountPointLimit )
            , mounts_( MountPointLimit )
            , paths_( MountPointLimit )
        {
        }


        VirtualVolumeImpl( VirtualVolumeImpl&& ) = delete;


        [[ nodiscard ]]
        auto Insert( KeyT path, KeyT subkey, ValueT && value, TimestampT && timestamp, bool overwrite ) noexcept
        {
            try
            {
                if ( path.is_path( ) && subkey.is_leaf( ) )
                {
                    return RetCode::NotImplementedYet;
                }
                else
                {
                    return RetCode::InvalidKey;
                }
            }
            catch(...)
            {
            }

            return RetCode::UnknownError;
        }


        [[ nodiscard ]]
        auto Get( const KeyT & key ) noexcept
        {
            using namespace std;

            try
            {
                return pair{ RetCode::NotImplementedYet, ValueT{} };
            }
            catch ( ... )
            {
            }

            return pair{ RetCode::UnknownError, ValueT{} };
        }


        [[ nodiscard ]]
        auto Erase( const KeyT & key, bool force ) noexcept
        {
            try
            {
                return RetCode::NotImplementedYet;
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        [[ nodiscard ]]
        auto Mount( PhysicalVolumeImplP physical_volume, const KeyT & physical_path, const KeyT & logical_path ) noexcept
        {
            using namespace std;

            try
            {
                assert( physical_volume );
                assert( physical_path.is_path( ) );
                assert( logical_path.is_path( ) );


                // start mounting
                unique_lock< shared_mutex > write_lock( mounts_guard_ );

                // if maximum number of mounts reached?
                if ( uids_.size() >= MountsLimit)
                {
                    return pair{ RetCode::LimitReached, MountPoint{} };
                }

                // prevent mouting of the same physical volume at a logicap path
                auto uid = misc::variadic_hash< Policies, Pad >( logical_path, physical_volume );
                if ( uids_.find( uid ) != uids_.end() )
                {
                    return pair{ RetCode::VolumeAlreadyMounted, MountPoint() };
                }

                // check that iterators won't be invalidated on insertion
                if ( !check_rehash() )
                {
                    assert( false ); // that's unexpected
                    return pair{ RetCode::UnknownError, MountPoint() };
                }

                // request mount from physical volume
                auto[ ret, mount ] = volume.get_mount( physical_path );
                if ( !mount )
                {
                    return pair{ ret, MountPoint() };
                }

                // TODO: consider using a static allocator
                auto backtrace = make_shared< MountPointBacktrace >(
                    move( MountPointBacktrace{ uids_.end(), mounts_.end(), paths_.end() } )
                    );

                try
                {
                    backtrace->uid_ = uids_.insert( { uid, backtrace } ).first;
                    backtrace->mount_ = mounts_.insert( { mount, backtrace } ).first;
                    backtrace->path_ = paths_.insert( { KeyHashF()( logical_path ), backtrace } );
                }
                catch ( ... ) // if something went wrong - rollback all
                {
                    auto rollback = [] ( auto container, auto it ) {
                        if ( it != container.end() ) container.erase( it );
                    };

                    rollback( uids_, backtrace->uid_ );
                    rollback( mounts_, backtrace->mount_ );
                    rollback( paths_, backtrace->path_ );

                    return pair{ RetCode::UnknownError, MountPoint{} };
                }

                return pair{ RetCode::Ok, MountPoint{ mount } };
            }
            catch ( ... )
            {
            }

            return pair{ RetCode::UnknownError, MountPoint{} };
        }
    };
}

#endif
