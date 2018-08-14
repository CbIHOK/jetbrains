#ifndef __JB__VIRTUAL_VOLUME_IMPL__H__
#define __JB__VIRTUAL_VOLUME_IMPL__H__


#include <unordered_set>
#include <unordered_map>
#include <shared_mutex>
#include <tuple>
#include <execution>
#include <boost/container/static_vector.hpp>
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
        using MountPointImplP = std::shared_ptr< MountPointImpl >;
       
        using Key = typename Storage::Key;
        using KeyValue = typename Storage::KeyValue;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;

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
            MountPointImplP,
            MountPointBacktraceP
        >;
        MountPointImplCollectionT mounts_;


        //
        // provides O(1) search my mount path, cover most of scenario
        //
        using MountedPathCollectionT = std::unordered_multimap< typename KeyValue, MountPointBacktraceP >;
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
            return check( uids_ ) & check( mounts_ ) & check( mounted_paths_ );
        }


        auto find_nearest_mounted_path( const Key & logical_path ) const
        {
            using namespace std;

            auto[ res, parent, rest ] = logical_path.split_at_tile( );
            assert( res );

            while ( parent != Key{} )
            {
                if ( mounted_paths_.find( parent ) != mounted_paths_.end( ) )
                {
                    return tuple{ parent, rest };
                }
            }

            return tuple{ Key{}, logical_path };
        }


        auto get_mount_points( const Key & path ) const
        {
            using namespace std;
            using namespace boost::container;

            // lock physical volume collection and get compare routine
            auto lesser_pv = Storage::get_lesser_priority();

            // get mount points by logical path
            auto[ from, to ] = mounted_paths_.equal_range( path );

            // vector on stack
            static_vector< MountPointImplP, MountsLimit > mount_points;

            // fill the collection with mount points
            for_each( from, to, [&] ( const auto & mount_point ) {
                MountPointBacktraceP backtrace = mount_point.second;
                MountPointImplP mount_point_impl = backtrace->mount_->first;
                mount_points.insert( mount_points.end(), mount_point_impl );
            } );

            // introduce comparing of mount points by physical volume priority
            auto lesser_mp = [&] ( const auto & mp1, const auto & mp2 ) {
                return lesser_pv( mp1->physical_volume(), mp2->physical_volume() );
            };

            // sort mount points by physical volume priority
            sort( execution::par, begin( mount_points ), end( mount_points ), lesser_mp );

            return mount_points;
        }



        auto unmount( const MountPoint & mp )
        {
        }


    public:

        VirtualVolumeImpl( ) : mounts_guard_( )
            , uids_( MountsLimit )
            , mounts_( MountsLimit )
            , mounted_paths_( MountsLimit )
        {
        }


        VirtualVolumeImpl( VirtualVolumeImpl&& ) = delete;


        [[ nodiscard ]]
        auto Insert( const Key & path, const Key & subkey, Value && value, Timestamp && good_before, bool overwrite )
        {
            using namespace std;

            assert( path.is_path() );
            assert( subkey.is_leaf() );

            try
            {
                shared_lock l( mounts_guard_ );

                auto[ mp_path, path_from_mp ] = find_nearest_mounted_path( path );
                auto mount_points = move( get_mount_points( mp_path ) );

                if ( mount_points.empty() )
                {
                    return tuple{ RetCode::InvalidLogicalPath };
                }

                return tuple{ RetCode::NotImplementedYet };
            }
            catch(...)
            {
            }

            return tuple{ RetCode::UnknownError };
        }


        [[ nodiscard ]]
        auto Get( const Key & key ) noexcept
        {
            using namespace std;

            try
            {
                return tuple{ RetCode::NotImplementedYet, Value{} };
            }
            catch ( ... )
            {
            }

            return tuple{ RetCode::UnknownError, Value{} };
        }


        [[ nodiscard ]]
        auto Erase( const Key & key, bool force ) noexcept
        {
            using namespace std;

            try
            {
                return tuple{ RetCode::NotImplementedYet };
            }
            catch ( ... )
            {
            }

            return tuple{ RetCode::UnknownError };
        }


        [[ nodiscard ]]
        auto Mount( PhysicalVolumeImplP physical_volume, const Key & physical_path, const Key & logical_path, const Key & alias ) noexcept
        {
            using namespace std;

            try
            {
                assert( physical_volume );
                assert( physical_path.is_path( ) );
                assert( logical_path.is_path( ) );
                assert( alias.is_leaf( ) );

                // start mounting
                unique_lock< shared_mutex > write_lock( mounts_guard_ );

                // if maximum number of mounts reached?
                if ( uids_.size() >= MountsLimit)
                {
                    return tuple{ RetCode::LimitReached, MountPoint{} };
                }

                // prevent mouting of the same physical volume at a logicap path
                auto uid = misc::variadic_hash< Policies, Pad >( logical_path, physical_volume );
                if ( uids_.find( uid ) != uids_.end() )
                {
                    return tuple{ RetCode::VolumeAlreadyMounted, MountPoint() };
                }

                // check that iterators won't be invalidated on insertion
                if ( !check_rehash() )
                {
                    assert( false ); // that's unexpected
                    return tuple{ RetCode::UnknownError, MountPoint() };
                }

                auto[ mp_path, path_from_mp ] = find_nearest_mounted_path( logical_path );

                return tuple{ RetCode::NotImplementedYet, MountPoint{} };
            }
            catch ( ... )
            {
            }

            return tuple{ RetCode::UnknownError, MountPoint{} };
        }
    };
}

#endif
