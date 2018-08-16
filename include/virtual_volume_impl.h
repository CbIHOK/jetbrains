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
        using MountPointImplWeakP = std::weak_ptr< MountPointImpl >;
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;
        //using execution_connector = typename MountPointImpl::execution_connector;
       
        using Key = typename Storage::Key;
        using KeyValue = typename Storage::KeyValue;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );

        static constexpr auto MountLimit = Policies::VirtualVolumePolicy::MountPointLimit;


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
        //   dependency     path <--> backtrace <--> mount     mount uid
        //       ^________________________|________________________^
        //
        // a forwarding declaration, see below for definition
        //
        struct MountPointBacktrace;
        using MountPointBacktraceP = std::shared_ptr< MountPointBacktrace >;


        //
        // holds unique mount description that takes into account logical path, physical volume, and
        // physical path. Let us to avoid identical mounts with O(1) complexity
        //
        using MountUid = size_t;
        using MountUidCollection = std::unordered_set< MountUid >;
        MountUidCollection uids_;


        //
        // provides O(1) search by mount PIMP pointer, cover dismount use case
        //
        using MountPointImplCollection = std::unordered_map <
            MountPointImplP,
            MountPointBacktraceP
        >;
        MountPointImplCollection mounts_;


        //
        // provides O(1) search my mount path hash
        //
        // the approach is to hold not paths but their hash values. That at the first reduces heap
        // defragmentation, and moreover places the data nearly in memory, i.e. that looks more
        // cache-friendly
        //
        using MountedPathCollection = std::unordered_multimap< KeyHashT, MountPointBacktraceP >;
        MountedPathCollection paths_;


        //
        // provides O(1) search from a mount point to underlaying mount points, i.e. preserves
        // dismounting a mount points that has dependent mount points
        //
        using MountDependecyCollection = std::unordered_multimap< KeyHashT, KeyHashT >;
        MountDependecyCollection dependencies_;


        //
        // just a postponed definition
        //
        struct MountPointBacktrace
        {
            typename MountUidCollection::const_iterator uid_;
            typename MountPointImplCollection::const_iterator mount_;
            typename MountedPathCollection::const_iterator path_;
            typename MountDependecyCollection::const_iterator dependency_;
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
            return check( uids_ ) && check( mounts_ ) && check( paths_ ) && check( dependencies_ );
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
                static Hash< Policies, Pad, Key > hasher{};

                auto current_hash = hasher( current );

                if ( paths_.count( current_hash ) )
                {
                    return tuple{ current, current_hash };
                }

                auto [ ret, superkey, subkey ] = current.split_at_tile( );
                assert( ret );

                current = move( superkey );
            }

            return tuple{ Key{}, KeyHashT{} };
        }


        /* Provides collection of Mounts for given logical path

        @param [in] logical_path - logical path
        @return vector< MountPointImplP > sorted in priority of their Physical Volumes
        */
        [[nodiscard]]
        auto get_mount_points( KeyHashT & mp_path_hash ) const
        {
            using namespace std;
            using namespace boost::container;

            // lock physical volume collection and get compare routine
            auto lesser_pv = Storage::get_lesser_priority();

            // get mount points by logical path
            auto[ from, to ] = paths_.equal_range( mp_path_hash );

            // vector on stack
            static_vector< MountPointImplP, MountLimit > mount_points;

            // fill the collection with mount points
            for_each( from, to, [&] ( const auto & mount_point ) {
                MountPointBacktraceP backtrace = mount_point.second;
                MountPointImplP mount_point_impl = backtrace->mount_->first;
                assert( mount_point_impl );
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
            , paths_( MountLimit )
            , dependencies_( MountLimit )
        {
        }


        VirtualVolumeImpl( VirtualVolumeImpl&& ) = delete;


        template < typename M, typename F >
        [ [ nodiscard ] ]
        auto run_parallel( const M & mounts, F f )
        {
            using namespace std;
            using namespace boost::container;

            using ContractT = decltype( f( MountPointImplP{}, pair< atomic_bool, atomic_bool >{}, pair< atomic_bool, atomic_bool >{} ) );
            using FutureT = future< ContractT >;

            // futures, one per mount
            static_vector< FutureT, MountLimit > futures;
            futures.resize( mounts.size( ) );

            // execution connectors are pairs of < cancel, do_it > flags
            static_vector< pair< atomic_bool, atomic_bool >, MountLimit > connectors;
            connectors.resize( mounts.size( ) + 1 );

            // through all mounts: connect the routines and start them asynchronuosly
            for ( auto mp_it = begin( mounts ); mp_it != end( mounts ); ++mp_it )
            {
                // ordinal number of mount
                auto d = static_cast< size_t >( distance( begin( mounts ), mp_it ) );

                // get mount implementation ptr
                auto mp = *mp_it;

                // assign future
                assert( d < futures.size( ) );
                auto & future = futures[ d ];

                // assign the connectors
                assert( d < connectors.size( ) && d + 1 < connectors.size( ) );
                auto & in = connectors[ d ];
                auto & out = connectors[ d + 1 ];

                // start routine
                future = async( launch::async, [&] ( ) noexcept { return f( mp, in, out ); } );
            }

            // let the 1st routine to DO IT
            connectors[ 0 ].second.store( true, memory_order_release );

            // wait for all futures
            for ( auto & future : futures ) { future.wait( ); }

            // return futures
            return futures;
        }


        [[ nodiscard ]]
        auto insert( const Key & path, const Key & subkey, Value && value, Timestamp && good_before, bool overwrite )
        {
            using namespace std;

            assert( path.is_path() );
            assert( subkey.is_leaf() );

            try
            {
                shared_lock l( mounts_guard_ );

                if ( auto[ mp_path, mp_path_hash ] = find_nearest_mounted_path( path ); mp_path )
                {
                    auto mount_points = move( get_mount_points( mp_path_hash ) );
                    assert( mount_points.size( ) );

                    return tuple{ RetCode::NotImplementedYet };
                }
                else
                {
                    return tuple{ RetCode::InvalidLogicalPath };
                }
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
                // TODO: consider locing only for collection mount points
                shared_lock l( mounts_guard_ );

                if ( auto[ mount_path, mount_path_hash ] = find_nearest_mounted_path( key ); mount_path )
                {
                    auto mount_points = move( get_mount_points( mount_path_hash ) );
                    assert( mount_points.size( ) );

                    return { RetCode::NotImplementedYet, Value{} };
                }
                else
                {
                    return { RetCode::InvalidLogicalPath, Value{} };
                }
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
            using namespace boost::container;

            assert( key.is_path( ) );

            try
            {
                // TODO: consider locking only for collection mount points
                shared_lock l( mounts_guard_ );

                if ( auto[ mount_path, mount_hash ] = find_nearest_mounted_path( key ); mount_path )
                {
                    auto mounts = move( get_mount_points( mount_hash ) );
                    assert( mounts.size() );

                    // get physical path as a rest from mount point
                    auto[ is_superkey, relative_path ] = mount_path.is_superkey( key );
                    assert( is_superkey );

                    // run erase() for all mounts in parallel
                    auto futures = move( run_parallel( mounts, [=] ( const auto & mount, const auto & in, auto & out ) noexcept {
                        return mount->erase( relative_path, in, out );
                    } ) );

                    // through all futures
                    for ( auto & future : futures )
                    {
                        // get result
                        auto[ ret ] = future.get( );

                        if ( RetCode::Ok == ret )
                        {
                            // done
                            return { RetCode::Ok };
                        }
                        else if ( RetCode::NotFound != ret )
                        {
                            // something happened on physical level
                            return { ret };
                        }
                    }

                    // key not found
                    return { RetCode::NotFound };
                }
                else
                {
                    return { RetCode::InvalidLogicalPath };
                }
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }


        /**
        */
        [[ nodiscard ]]
        std::tuple< RetCode, MountPoint > mount (   PhysicalVolumeImplP physical_volume, 
                                                    const Key & physical_path,
                                                    const Key & logical_path,
                                                    const Key & alias   ) noexcept
        {
            using namespace std;
            using namespace boost::container;

            assert( physical_volume );
            assert( physical_path.is_path( ) );
            assert( logical_path.is_path( ) );
            assert( alias.is_leaf( ) );

            try
            {
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

                // will lock path to mount
                NodeLock lock_mount_to;

                // find nearest upper mount point
                auto[ parent_path, parent_hash ] = find_nearest_mounted_path( logical_path );

                // if we're mounting under another mount we need to lock corresponding path on physical level
                if ( parent_path )
                {
                    // get mount points for logical path
                    auto parent_mounts = move( get_mount_points( parent_hash ) );
                    assert( parent_mounts.size() );

                    // get physical path as a rest from mount point
                    auto[ is_superkey, relative_path ] = parent_path.is_superkey( logical_path );
                    assert( is_superkey );

                    // run lock_path() for all parent mounts in parallel
                    auto futures = move( run_parallel( parent_mounts, [=] ( const auto & mount, const auto & in, auto & out ) noexcept {
                        return mount->lock_path( relative_path, in, out );
                    } ) );

                    // init overall status as NotFound
                    auto overall_status = RetCode::NotFound;

                    // through all futures
                    for ( auto & future : futures )
                    {
                        // get result
                        auto[ ret, node_uid, lock ] = future.get();

                        if ( RetCode::Ok == ret )
                        {
                            // get lock over logical path we're going mount to
                            lock_mount_to = move( lock );
                            overall_status = RetCode::Ok;
                            break;
                        }
                        else if ( RetCode::NotFound != ret )
                        {
                            // something terrible happened on physical level
                            overall_status = ret;
                        }
                    }

                    // unable to lock physical path
                    if ( RetCode::Ok != overall_status )
                    {
                        return { overall_status, MountPoint{} };
                    }
                }

                // create mounting point
                auto mp = make_shared< MountPointImpl >( physical_volume, physical_path, move( lock_mount_to ) );

                // if mounting successful
                if ( mp->status() != RetCode::Ok )
                {
                    return { mp->status(), MountPoint{} };
                }

                // create backtrace record
                auto backtrace_ptr = make_shared< MountPointBacktrace >();

                // combine logical path for new mount
                auto mounted_hash = Hash< Policies, Pad, KeyValue >{}( logical_path / alias );

                // insert all the keys and fill backtrace
                backtrace_ptr->uid_ = uids_.insert( uid ).first;
                backtrace_ptr->mount_ = mounts_.insert( { mp, backtrace_ptr } ).first;
                backtrace_ptr->path_ = paths_.insert( { mounted_hash, backtrace_ptr } );
                backtrace_ptr->dependency_ = parent_path ? dependencies_.insert( { parent_hash, mounted_hash } ) : dependencies_.end();

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
