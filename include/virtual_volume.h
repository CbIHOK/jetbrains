#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include "physical_volume.h"
#include "mount_point.h"
#include "rare_write_frequent_read_mutex.h"
#include "path_iterator.h"
#include "path_utils.h"
#include <string>
#include <string_view>
#include <memory>
#include <tuple>
#include <execution>
#include <future>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/composite_key.hpp>


namespace jb
{
    template < typename Policies, typename TestHooks >
    class Storage;


    template < typename Policies >
    class VirtualVolume
    {
        template < typename Policies > class Storage;

        using Key = std::basic_string< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
        using KeyView = std::basic_string_view< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
        using Value = typename Policies::Value;
        using MountPoint = ::jb::MountPoint< Policies >;
        using MountPointPtr = std::shared_ptr< MountPoint >;
        using MountPointHandle = std::weak_ptr< MountPoint >;
        using PhysicalVolume = ::jb::PhysicalVolume< Policies >;
        using PhysicalVolumeHandle = std::weak_ptr< PhysicalVolume >;


        struct priority_compare
        {
            bool operator () ( MountPointPtr lhs, MountPointPtr rhs ) const _NOEXCEPT
            {
                assert( lhs && lhs->physical_volume() );
                assert( rhs && rhs->physical_volume() );

                return lhs->logical_path_ < rhs->logical_path_ ||
                    lhs->logical_path_ == rhs->logical_path_ && lhs->physical_volume()->priority() > rhs->physical_volume()->priority() ||
                    lhs->logical_path_ == rhs->logical_path_ && lhs->physical_volume()->priority() == rhs->physical_volume()->priority() && lhs->priority() < rhs->priority();
            }

            bool operator () ( KeyView lhs, MountPointPtr rhs ) const _NOEXCEPT
            { 
                assert( rhs );
                return lhs < rhs->logical_path(); 
            }

            bool operator () ( MountPointPtr lhs, KeyView rhs ) const _NOEXCEPT
            {
                assert( lhs );
                return lhs->logical_path() < rhs;
            }
        };


        struct ndx_logical_path {};
        struct ndx_uniqueness {};
        struct ndx_children {};
        struct ndx_priority {};


        using mount_collection_t = boost::multi_index_container<
            MountPointPtr,
            boost::multi_index::indexed_by<
                // provides O(0) search of mount point by logical path
                boost::multi_index::hashed_non_unique<
                    boost::multi_index::tag< ndx_logical_path >,
                    boost::multi_index::const_mem_fun< MountPoint, KeyView, &MountPoint::logical_path >
                >,
                // prevents repeat mounting of the same physical volume to the same logical path to avoid deadlock
                boost::multi_index::hashed_unique<
                    boost::multi_index::tag< ndx_uniqueness >,
                    boost::multi_index::composite_key<
                        MountPointPtr,
                        boost::multi_index::const_mem_fun< MountPoint, KeyView, &MountPoint::logical_path >,
                        boost::multi_index::mem_fun< MountPointPtr, MountPoint*, &MountPointPtr::get >
                    >
                >,
                // provides O(0) selection of children mounts by parent
                boost::multi_index::hashed_non_unique<
                    boost::multi_index::tag< ndx_children >,
                    boost::multi_index::const_mem_fun< MountPoint, MountPointPtr, &MountPoint::parent >
                >,
                // provides O( log N ) selection of mounts points by logical path
                boost::multi_index::ordered_unique<
                    boost::multi_index::tag< ndx_priority >,
                    boost::multi_index::identity< MountPointPtr >,
                    priority_compare
                >
            >
        >;

        rare_write_frequent_read_mutex<> guard_;
        mount_collection_t mounts_;
        RetCode status_ = Ok;


        /* Finds nearest mount for given logical path

        @param [in] logical path
        @retval Key - nearest mounted path
        @retval KeyHashT - hash value of nearest mounted path
        @throw nothing
        */
        std::tuple< KeyView, KeyView > find_nearest_mounted_path( KeyView logical_path ) const _NOEXCEPT
        {
            const auto & by_logical_path = mounts_.get< mount_t::ndx_logical_path >();
            
            const auto begin = path_begin( logical_path );
            const auto end = path_end( logical_path );

            for ( auto it = end; it != begin; --it )
            {
                const auto prefix = it - begin;
                const auto suffix = end - it;

                if ( by_logical_path.count( prefix ) )
                {
                    return { prefix, suffix };
                }
            }

            return { KeyView{}, logical_path };
        }


        template < typename F >
        auto for_each_mount( KeyView mount_path, F f )
        {
            // count mounts for given mount path
            const auto & by_logical_path = mounts_.get< mount_t::by_logical_path >();
            auto mount_count = by_logical_path.count( mount_path );
            assert( mount_count );

            // acquire mounts for the path
            const auto & by_priority = mounts_.get< mount_t::by_priority >();
            auto[ mount_it, mount_end ] = by_priority.equal_range( mount_path );

            // allocate memory
            using FutureT = std::future< decltype( f( MountPointPtr{}, std::shared_future<, std::nullptr ) ) >;
            std::vector< std::tuple< MountPointPtr, FutureT > > mounts( mount_count );
            std::vector< std::promise< bool > > promises( mount_count + 1 );

            // for all mounts
            auto f = futures.begin();
            auto e = execution.begin();
            for ( ; mount_it != mount_end; ++mount_it, ++f, ++e )
            {
                assert( futures.end() != f );
                assert( execution.end() != e );
                assert( execution.end() != e + 1 );

                f->get< MountPointPtr >() = mount_it->impl_;
                f->get< FutureT >() = std::move( std::async( std::launch::async, [&] () noexcept { return f( *m, e, e + 1 ); } ) );
            }

            // let the 1st thread to apply an operation 
            execution.front().allow();

            // wait until the last thread gets completed
            execution.back().wait_until_previous_completed();

            // return 
            return futures;
        }


        /** Default constructor

        @throw may throw std::exception by some reason
        */
        VirtualVolume() _NOEXCEPT {}


        /** The class is not copyable/movable
        */
        VirtualVolume( VirtualVolume&& ) = delete;


        RetCode status() const _NOEXCEPT
        {
            return status_;
        }


    public:

        /** Inserts subkey of a given name with given value and expiration timemark at specified logical path

        @params
        @throw nothing
        */
        RetCode insert( const Key & path, const Key & subkey, Value && val, uint64_t good_before = 0, bool overwrite = 0 ) _NOEXCEPT
        {
            try
            {
                if ( !is_valid_path( path ) )
                {
                    return InvalidKey;
                }

                if ( !is_valid_path_segment( subkey ) )
                {
                    return InvalidSubkey;
                }

                if ( good_before && good_before < std::chrono::system_clock::now().time_since_epoch() / 1ms )
                {
                    return RetCode::AlreadyExpired;
                }

                rare_write_frequent_read_mutex<>::shared_lock<> s_lock( guard_ );

            }
            catch ( const std::bad_alloc & )
            {
                return { InsufficientMemory, Value{} };
            }
            catch ( ... )
            {
                return { UnknownError, Value{} };
            }

            using namespace std;

            assert( path.is_path() );
            assert( subkey.is_leaf() );

            // TODO: consider locking only for collection mount points
            shared_lock l( mounts_guard_ );

            if ( auto[ mount_path, mount_hash ] = find_nearest_mounted_path( path ); mount_path )
            {
                // get mounts for the key
                auto mounts = move( get_mount_points( mount_hash ) );
                assert( mounts.size( ) );

                // get relative path as a rest from mount point
                auto[ is_superkey, relative_path ] = mount_path.is_superkey( path );

                // run insert() for all mounts in parallel
                auto futures = std::move( run_parallel( mounts, [&] ( const auto & mount, const auto & in, auto & out ) noexcept {
                    return mount->insert( relative_path, subkey, value, good_before, overwrite, in, out );
                } ) );

                // through all futures
                for ( auto & future : futures )
                {
                    // get result
                    auto ret = future.get( );

                    if ( RetCode::Ok == ret )
                    {
                        // done
                        return RetCode::Ok;
                    }
                    else if ( RetCode::NotFound != ret )
                    {
                        // something happened on physical level
                        return ret;
                    }
                }

                // key not found
                return RetCode::NotFound;
            }
            else
            {
                return RetCode::InvalidLogicalPath;
            }
        }


        [[ nodiscard ]]
        std::tuple< RetCode, Value > get( const KeyView & key ) _NOEXCEPT
        {
            try
            {
                if ( is_valid_path( key ) )
                {
                    rare_write_frequent_read_mutex<>::shared_lock<> s_lock( guard_ );

                    if ( auto[ mount_path, relative_path ] = find_nearest_mounted_path( key ); mount_path.size() )
                    {
                        auto futures = for_each_mount( mount_path, [&] ( auto mount, auto * in, auto * out ) noexcept { return mount->get( relative_path, in, out ); } );

                        for ( auto & future : futures )
                        {
                            if ( auto[ rc, value ] = future.get< std::tuple< RetCode, Value > >();  Ok == rc )
                            {
                                return { Ok, value }
                            }
                            else if ( NotFound != rc )
                            {
                                return { rc, Value{} };
                            }
                        }
                    }

                    return { NotFound, Value{} };
                }
                else
                {
                    return InvalidKey;
                }
            }
            catch ( const std::bad_alloc & )
            {
                return { InsufficientMemory, Value{} };
            }
            catch ( ... )
            {
                return { UnknownError, Value{} };
            }
        }


        RetCode erase( const Key & key, bool force ) _NOEXCEPT
        {
            try
            {
                if ( !detail::is_valid_path( key ) )
                {
                    return InvalidKey;
                }
                else
                {
                    rare_write_frequent_read_mutex<>::shared_lock s_lock( guard_ );

                    if ( auto[ mount_path, relative_path ] = find_nearest_mounted_path( key ); mount_path.size() )
                    {
                    }

                    return NotFound;
                }
            }
            catch ( const std::bad_alloc & )
            {
                return InsufficientMemory;
            }
            catch ( ... )
            {
                return UnknownError;
            }
        }


        /** Mounts given physical path from given physical volume at a logical path with givel alias

        @param [in] PhysicalVolume - physical volume to be mounted
        @param [in] physical_path - physical path to be mounted
        @param [in] logical_path - physical volume to be mounted
        @retval RetCode - operation status
        @retval MountPoint - if operation succeeded contains valid handle of mounting 
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode, MountPointHandle > mount ( PhysicalVolumeHandle physical_volume,
                                                        const Key & physical_path,
                                                        Key && logical_path ) _NOEXCEPT
        {
            try
            {
                if ( auto pv = physical_volume.lock(); !pv )
                {
                    return { InvalidPhysicalVolume, MountPointHandle{} };
                }
                else if ( !detail::is_valid_path( physical_path ) )
                {
                    return { InvalidPhysicalPath, MountPointHandle{} };
                }
                else if ( !detail::is_valid_path( logical_path ) )
                {
                    return { InvalidLogicalPath, MountPointHandle{} };
                }
                else if ( auto mount_point = std::make_shared< MountPoint >( physical_volume, physical_path, logical_path ); Ok != mount_point->status() )
                {
                    return mount_point->status();
                }
                else
                {
                    return { Ok, MountPointHandle{ mount_point } };
                }
            }
            catch ( const bad_alloc & )
            {
                return { RetCode::InsufficientMemory, MountPointImplP{} };
            }
            catch ( ... )
            {
                return { RetCode::UnknownError}
            }
        }


        /** Unmount given mount point

        @param [in] mp - mount point
        @param [in] force - unmount underlaying mounts
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode unmount( std::weak_ptr< MountPoint > mount_handle, bool force = false ) _NOEXCEPT
        {
            //try
            //{
            //    rare_write_frequent_read_mutex<>::unique_lock<> x_lock( guard_ );

            //    auto & by_pimp = mounts_.get< mount_t::by_pimp >();
            //    auto & by_parent = mounts_.get< mount_t::by_parent >();

            //    auto impl = mount_handle.lock();
            //    if ( !impl )
            //    {
            //        return RetCode::InvalidHandle;
            //    }

            //    // look for mount to be removed
            //    auto by_pimp_it = by_pimp.find( impl );
            //    assert( by_pimp.end() != by_pimp_it );

            //    // initialize collection of mounts to be removed
            //    std::vector< decltype( by_pimp_it ) > to_remove;
            //    to_remove.reserve( mounts_.size() );
            //    to_remove.push_back( by_pimp_it );
            //    auto children_gathered = to_remove.begin();

            //    // all children gathered
            //    while ( to_remove.end() != children_gathered )
            //    {
            //        // get mount PIMP
            //        auto parent = ( *children_gathered++ )->pimp_;

            //        // select children mounts
            //        auto[ children_it, children_end ] = by_parent.equal_range( parent );

            //        // and mark them to remove
            //        while ( children_it != children_end )
            //        {
            //            by_pimp_it = mounts_.project< mount_t::by_pimp >( children_it++ );
            //            assert( by_pimp.end() != by_pimp_it );

            //            to_remove.push_back( by_pimp_it );
            //        }
            //    }

            //    // if there are more than 1 mount to be removed
            //    if ( to_remove.size() > 1 && !force )
            //    {
            //        // we have children mouns
            //        return RetCode::HasDependentMounts;
            //    }

            //    // remove all the mounts
            //    for ( auto & removing_it : to_remove )
            //    {
            //        by_pimp.erase( removing_it );
            //    }

            //    return RetCode::Ok;
            //}
            //catch ( ... )
            //{
            //    return UnknownError;
            //}
            return UnknownError;
        }
    };
}

#endif
