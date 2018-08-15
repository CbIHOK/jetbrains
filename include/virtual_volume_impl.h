#ifndef __JB__VIRTUAL_VOLUME_IMPL__H__
#define __JB__VIRTUAL_VOLUME_IMPL__H__


#include <unordered_set>
#include <unordered_map>
#include <shared_mutex>
#include <tuple>
#include <execution>
#include <future>
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
        using NodeLock = typename PhysicalVolumeImpl::NodeLock;
        using MountPoint = typename Storage::MountPoint;
        using MountPointImpl = typename Storage::MountPointImpl;
        using MountPointImplP = std::shared_ptr< MountPointImpl >;
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;
        using execution_connector = typename MountPointImpl::execution_connector;
       
        using Key = typename Storage::Key;
        using KeyValue = typename Storage::KeyValue;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;

        static constexpr size_t MountLimit = Policies::VirtualVolumePolicy::MountPointLimit;


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


        /* Finds nearest mounted path for 
        */
        [[nodiscard]]
        auto find_nearest_mounted_path( const Key & logical_path ) const
        {
            using namespace std;

            auto current = logical_path;

            while ( current != Key{} )
            {
                if ( mounted_paths_.count( current ) )
                {
                    break;
                }

                auto [ ret, superkey, subkey ] = current.split_at_tile( );
                assert( ret );

                current = move( superkey );
            }

            return tuple{ current };
        }


        /* Provides collection of Mounts for given logical path

        @param [in] logical_path - logical path
        @return vector< MountPointImplP > sorted in priority of their Physical Volumes
        */
        [[nodiscard]]
        auto get_mount_points( const Key & logical_path ) const
        {
            using namespace std;
            using namespace boost::container;

            // lock physical volume collection and get compare routine
            auto lesser_pv = Storage::get_lesser_priority();

            // get mount points by logical path
            auto[ from, to ] = mounted_paths_.equal_range( logical_path );

            // vector on stack
            static_vector< MountPointImplP, MountLimit > mount_points;

            // fill the collection with mount points
            for_each( from, to, [&] ( const auto & mount_point ) {
                MountPointBacktraceP backtrace = mount_point.second;
                MountPointImplP mount_point_impl = backtrace->mount_->first;
                mount_points.emplace( mount_points.end(), mount_point_impl );
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
            , uids_( MountLimit )
            , mounts_( MountLimit )
            , mounted_paths_( MountLimit )
        {
        }


        VirtualVolumeImpl( VirtualVolumeImpl&& ) = delete;


        [[ nodiscard ]]
        auto insert( const Key & path, const Key & subkey, Value && value, Timestamp && good_before, bool overwrite )
        {
            using namespace std;

            assert( path.is_path() );
            assert( subkey.is_leaf() );

            try
            {
                shared_lock l( mounts_guard_ );

                auto[ mp_path ] = find_nearest_mounted_path( path );
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
        std::tuple< RetCode, Value > get( const Key & key ) noexcept
        {
            using namespace std;

            assert( key.is_path( ) );

            try
            {
                shared_lock l( mounts_guard_ );

                auto[ mp_path ] = find_nearest_mounted_path( key );
                auto mount_points = move( get_mount_points( mp_path ) );

                if ( mount_points.empty( ) )
                {
                    return { RetCode::InvalidLogicalPath, Value{} };
                }

                return { RetCode::NotImplementedYet, Value{} };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, Value{} };
        }


        [[ nodiscard ]]
        std::tuple< RetCode > erase( const Key & key, bool force ) noexcept
        {
            using namespace std;

            try
            {
                shared_lock l( mounts_guard_ );

                auto[ mp_path ] = find_nearest_mounted_path( key );
                auto mount_points = move( get_mount_points( mp_path ) );

                if ( mount_points.empty( ) )
                {
                    return { RetCode::InvalidLogicalPath, };
                }

                return { RetCode::NotImplementedYet };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }


        [[ nodiscard ]]
        std::tuple< RetCode, MountPoint > mount( PhysicalVolumeImplP physical_volume, 
                                                 const Key & physical_path,
                                                 const Key & logical_path,
                                                 const Key & alias ) noexcept
        {
            using namespace std;
            using namespace boost::container;

            try
            {
                assert( physical_volume );
                assert( physical_path.is_path( ) );
                assert( logical_path.is_path( ) );
                assert( alias.is_leaf( ) );

                // lock over all mounts
                unique_lock< shared_mutex > write_lock( mounts_guard_ );

                // if maximum number of mounts reached?
                if ( mounts_.size() >= MountLimit)
                {
                    return { RetCode::LimitReached, MountPoint{} };
                }

                // prevent mouting of the same physical volume at a logicap path
                auto uid = misc::variadic_hash< Policies, Pad >( logical_path, physical_volume );
                if ( uids_.find( uid ) != uids_.end() )
                {
                    return { RetCode::VolumeAlreadyMounted, MountPoint() };
                }

                // check that iterators won't be invalidated on insertion
                if ( !check_rehash() )
                {
                    assert( false ); // that's unexpected
                    return { RetCode::UnknownError, MountPoint() };
                }

                // find nearest mount point for logical path
                auto mp_path = std::get< Key >( find_nearest_mounted_path( logical_path ) );

                // will lock path to mount
                NodeLock lock_mount_to;

                // if we're mounting under another mount we need to lock corresponding path on physical level
                if ( mp_path != Key{} )
                {
                    // get mount points for logical path
                    auto mount_points = move( get_mount_points( mp_path ) );
                    assert( mount_points.size() );

                    // get physical path as a rest from mount point
                    auto[ is_superkey, relative_path ] = mp_path.is_superkey( logical_path );
                    assert( is_superkey );

                    // futures, one per mount
                    static_vector< future< tuple< RetCode, NodeUid, NodeLock > >, MountLimit > futures;
                    futures.resize( mount_points.size() );

                    // execution connectors are pairs of < cancel, do_it > flags
                    static_vector< pair< atomic_bool, atomic_bool >, MountLimit > connectors;
                    connectors.resize( mount_points.size() + 1 );

                    // through all mounts: start locking routine
                    for ( auto mp_it = begin( mount_points ); mp_it != end( mount_points ); ++mp_it )
                    {
                        auto d = static_cast< size_t >( distance( begin( mount_points ), mp_it ) );
                         
                        MountPointImplP mp = *mp_it;
                        
                        assert( d < futures.size() );
                        auto & future = futures[ d ];

                        assert( d < connectors.size() && d + 1 < connectors.size() );
                        auto & in = connectors[ d ];
                        auto & out = connectors[ d + 1 ];

                        future = async( std::launch::async, [&] { return mp->lock_path( relative_path, in, out ); } );
                    }

                    // wait for all futures
                    for ( auto & future : futures ) { future.wait(); }

                    // through all futures
                    for ( auto & future : futures )
                    {
                        // get result
                        auto[ ret, node_uid, lock ] = future.get();

                        if ( RetCode::Ok == ret )
                        {
                            // get lock over the path we're going mount to
                            lock_mount_to = move( lock );
                            break;
                        }
                        else if ( RetCode::NotFound != ret )
                        {
                            // something happened on physical level
                            return { ret, MountPoint{} };
                        }
                    }

                    // target path for mounting does not exist
                    return { RetCode::NotFound, MountPoint{} };
                }

                // create mounting point
                MountPointImplP mp = make_shared< MountPointImpl >( physical_volume, physical_path, move( lock_mount_to ) );

                // if mounting successful
                if ( mp->status() != RetCode::Ok )
                {
                    return { mp->status(), MountPoint{} };
                }

                // create backtrace record
                MountPointBacktraceP backtrace_ptr = make_shared< MountPointBacktrace >();
                auto backtrace = * backtrace_ptr;

                // combine logical path for new mount
                KeyValue mounted_path = logical_path / alias;

                // fill backtrace
                backtrace.uid_ = uids_.emplace( uid, backtrace_ptr ).first;
                backtrace.mount_ = mounts_.emplace( mp, backtrace_ptr ).first;
                backtrace.path_ = mounted_paths_.emplace( move( mounted_path ), backtrace_ptr );

                // done
                return { RetCode::Ok, MountPoint{ mp } };
            }
            catch ( const bad_alloc & )
            {
                return { RetCode::InsufficientMemory, MountPoint{} };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, MountPoint{} };
        }
    };
}

#endif
