#ifndef __JB__VirtualVolume__H__
#define __JB__VirtualVolume__H__

#include "rare_write_frequent_read_mutex.h"
#include "execution_chain.h"
#include <string>
#include <string_view>
#include <memory>
#include <tuple>
#include <execution>
#include <future>
#include <regex>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/composite_key.hpp>


namespace jb
{

    template < typename Policies > class mount_point;


    template < typename Policies >
    class VirtualVolume
    {
        using Key = std::basic_string< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
        using KeyView = std::basic_string_view< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
        using Value = typename Policies::Value;
        using MountPoint = ::jb::mount_point< Policies >;
        using MountPointPtr = std::shared_ptr< MountPoint >;
        using PhysicalVolume = ::jb::PhysicalVolume< Policies >;

        struct mount_t
        {
            Key logical_path_;
            MountPointPtr impl_;
            MountPointPtr parent_;

            struct ndx_by_logical_path {};
            struct ndx_by_logical_path_and_PhysicalVolume {};
            struct ndx_by_impl {};
            struct ndx_by_parent {};
            struct ndx_by_priority {};

            explicit mount_t( Key && logical_path, MountPointPtr impl, MountPointPtr parent )
                : logical_path_( logical_path )
                , impl_( impl )
                , parent_( parent_impl )
            {
                assert( impl_ );
            }

            size_t PhysicalVolume() const noexcept
            {
                assert( impl_ );
                return impl_->PhysicalVolume();
            }

            size_t PhysicalVolume_priority() const noexcept
            {
                assert( PhysicalVolume() );
                return PhysicalVolume()->priority();
            }

            size_t mount_point_priority() const noexcept
            {
                assert( impl_ );
                return impl_->priority();
            }
        };


        struct priority_compare
        {
            bool operator () ( const mount_t & lhs, const mount_t & rhs ) const noexcept
            {
                return ( lhs.logical_path_ < rhs.logical_path_ )
                    ||
                    (
                        lhs.logical_path_ == rhs.logical_path_ &&
                        lhs.PhysicalVolume_priority() > rhs.PhysicalVolume_priority()
                    )
                    ||
                    (
                        lhs.logical_path_ == rhs.logical_path_ &&
                        lhs.PhysicalVolume_priority() == rhs.PhysicalVolume_priority() &&
                        lhs.mount_point_priority() < rhs.mount_point_priority()
                    );
            }

            bool operator () ( KeyView lhs, const mount_t & rhs ) const noexcept
            {
                return lhs < rhs.logical_path_;
            }

            bool operator () ( const mount_t & lhs, KeyView rhs ) const noexcept
            {
                return lhs.logical_path_ < rhs;
            }
        };


        using mount_collection_t = boost::multi_index_container<
            mount_t,
            boost::multi_index::indexed_by<
                // provides O(0) search of mount point by logical path
                boost::multi_index::hashed_non_unique<
                    boost::multi_index::tag< typename mount_t::ndx_by_logical_path >,
                    boost::multi_index::member< mount_t, Key, &mount_t::logical_path_ >
                >,
                // prevents repeat mounting of the same physical volume to the same logical path to avoid deadlock
                boost::multi_index::hashed_unique<
                    boost::multi_index::tag< typename mount_t::ndx_by_logical_path_and_PhysicalVolume >,
                    boost::multi_index::composite_key<
                        mount_t,
                        boost::multi_index::member< mount_t, Key, &mount_t::logical_path_ >,
                        boost::multi_index::mem_fun< mount_t, MountPointPtr, &mount_t::PhysicalVolume >
                    >
                >,
                // provides O(0) search by mount point PIMP
                boost::multi_index::hashed_unique<
                    boost::multi_index::tag< typename mount_t::ndx_by_impl >,
                    boost::multi_index::member< mount_t, MountPointPtr, &mount_t::impl_ >
                >,
                // provides O(0) selection of children mount points
                boost::multi_index::hashed_non_unique<
                    boost::multi_index::tag< typename mount_t::ndx_by_parent >,
                    boost::multi_index::member< mount_t, MountPointPtr, &mount_t::parent_ >
                >,
                // provides O( log N ) selection of mounts points by logical path
                boost::multi_index::ordered_unique<
                    boost::multi_index::tag< typename mount_t::ndx_by_priority >,
                    boost::multi_index::identity< mount_t >,
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
        std::tuple< KeyView, KeyView > find_nearest_mounted_path( KeyView logical_path ) const noexcept
        {
            const auto & by_logical_path = mounts_.get< mount_t::ndx_by_logical_path >();
            auto current = logical_path;

            while ( current.size() )
            {
                if ( by_logical_path.end() != by_logical_path.find( current ) )
                {
                    break;
                }

                static constexpr key_view::value_type separator = '/';
                auto prev_separator_pos = current.find_last_of( separator );
                assert( prev_separator_pos != key_view::npos );

                current = current.substr( 0, prev_separator_pos );
            }

            return { current, logical_path.substr( current.size() ) };
        }


        //template < typename F >
        //auto for_each_mount( key_view mount_path, F f )
        //{
        //    // count mounts for given mount path
        //    const auto & by_logical_path = mounts_.get< mount_t::by_logical_path >();
        //    auto mount_count = by_logical_path.count( mount_path );
        //    assert( mount_count );

        //    // acquire mounts for the path
        //    const auto & by_priority = mounts_.get< mount_t::by_priority >();
        //    auto[ mount_it, mount_end ] = by_priority.equal_range( mount_path );

        //    // allocate memory
        //    using FutureT = std::future< decltype( f( MountPointPtr{}, std::shared_future<, std::nullptr ) ) >;
        //    std::vector< std::tuple< MountPointPtr, FutureT > > mounts( mount_count );
        //    std::vector< std::promise< bool > > promises( mount_count + 1 );

        //    // for all mounts
        //    auto f = futures.begin();
        //    auto e = execution.begin();
        //    for ( ; mount_it != mount_end; ++mount_it, ++f, ++e )
        //    {
        //        assert( futures.end() != f );
        //        assert( execution.end() != e );
        //        assert( execution.end() != e + 1 );

        //        f->get< MountPointPtr >() = mount_it->impl_;
        //        f->get< FutureT >() = std::move( std::async( std::launch::async, [&] () noexcept { return f( *m, e, e + 1 ); } ) );
        //    }

        //    // let the 1st thread to apply an operation 
        //    execution.front().allow();

        //    // wait until the last thread gets completed
        //    execution.back().wait_until_previous_completed();

        //    // return 
        //    return futures;
        //}


    public:

        using Key;
        using Value;
        using MountPoint;

        /** Default constructor

        @throw may throw std::exception by some reason
        */
        VirtualVolume() : noexcept
        {
            mounts_.reserve( 64 );
        }


        /** The class is not copyable/movable
        */
        VirtualVolume( VirtualVolume&& ) = delete;


        RetCode status() const noexcept
        {
            return status_;
        }

        /** Inserts subkey of a given name with given value and expiration timemark at specified logical path

        @params
        @throw nothing
        */
        RetCode insert( const Key & path, const Key & subkey, Value && val, uint64_t good_before = 0, bool overwrite = 0 ) noexcept
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
        std::tuple< RetCode, Value > get( const KeyView & key ) noexcept
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


        RetCode erase( const KeyView & key, bool force ) noexcept
        {
            try
            {
                if ( !is_valid_path( key ) )
                {
                    rare_write_frequent_read_mutex<>::shared_lock<> s_lock( guard_ );

                    if ( auto[ mount_path, relative_path ] = find_nearest_mounted_path( key ); mount_path.size() )
                    {
                        auto futures = for_each_mount( mount_path, [&] ( auto mount, auto * in, auto * out ) noexcept { return mount->erase( relative_path, in, out ); } );

                        for ( auto & future : futures )
                        {
                            if ( auto rc = future.get< RetCode >(); NotFound != rc )
                            {
                                return rc;
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


        /** Mounts given physical path from given physical volume at a logical path with givel alias

        @param [in] PhysicalVolume - physical volume to be mounted
        @param [in] physical_path - physical path to be mounted
        @param [in] logical_path - physical volume to be mounted
        @retval RetCode - operation status
        @retval MountPoint - if operation succeeded contains valid handle of mounting 
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode, std::weak_ptr< MountPoint > >
        mount (  std::weak_ptr< PhysicalVolume > PhysicalVolume_handle, 
                 std::basic_string_view< typename Policies::KeyCharT > physical_path,
                 std::basic_string_view< typename Policies::KeyCharT > logical_path,
                 const KeyView & destination_logical_path,
                 const KeyView & mount_alias   ) noexcept
        {
            try
            {
                rare_write_frequent_read_mutex<>::shared_lock s_lock( guard_ );

                // find nearest upper mount point
                auto[ parent_mount_path, relative_path ] = find_nearest_mounted_path( logical_path );

                PathLock dst_lock;
                MountPointImplP parent_mount_impl;

                // if we're mounting under another mount we need to lock corresponding path on physical level
                if ( parent_mount_path )
                {
                    const auto parent_mount_count = count_mount_point( parent_mount_path );

                    std::vector< MountPointImplP > mount_points();
                    parent_mount_points.reserve( mount_count );

                    //// run lock_path() for all parent mounts in parallel
                    //auto futures = move( run_parallel( parent_mounts, [&] ( const auto & mount, const auto & in, auto & out ) noexcept {
                    //    return mount->lock_path( relative_path, in, out );
                    //} ) );

                    //// init overall status as NotFound
                    //auto overall_status = RetCode::NotFound;

                    //// through all futures
                    //for ( auto & future : futures )
                    //{
                    //    // get result
                    //    auto[ ret, node_uid, node_level, lock ] = future.get();

                    //    if ( RetCode::Ok == ret )
                    //    {
                    //        // get lock over logical path we're going mount to
                    //        lock_mount_to = move( lock );
                    //        overall_status = RetCode::Ok;
                    //        break;
                    //    }
                    //    else if ( RetCode::NotFound != ret )
                    //    {
                    //        // something terrible happened on physical level
                    //        overall_status = ret;
                    //    }
                    //}

                    //// unable to lock physical path
                    //if ( RetCode::Ok != overall_status )
                    //{
                    //    return { overall_status, MountPointImplP{} };
                    //}
                }
                
                if ( auto mount_point_impl = std::make_shared< MountPointImpl >( PhysicalVolume, physical_path, PathLock{} ); RetCode::Ok == mp->status() )
                {
                    KeyValue mounted_path;
                    mounted_path.reserve( destination_logical_path.size() + mount_alias.size() + 1 );
                    std::for_each( destination_logical_path.begin(), destination_logical_path.end(), std::back_inserter( mounted_path ) );
                    mounted_path.append( separator );
                    std::for_each( mount_alias.begin(), mount_alias.end(), std::back_inserter( mounted_path ) );

                    mounts_.emplace( mounted_path, mount_point_impl, parent_mount_impl );
                }
                else
                {
                    return { mp->status(), MountPointImpl{} };
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
        RetCode unmount( std::weak_ptr< mount_point > mount_handle, bool force = false ) noexcept
        {
            try
            {
                rare_write_frequent_read_mutex<>::unique_lock<> x_lock( guard_ );

                auto & by_pimp = mounts_.get< mount_t::by_pimp >();
                auto & by_parent = mounts_.get< mount_t::by_parent >();

                auto impl = mount_handle.lock();
                if ( !impl )
                {
                    return RetCode::InvalidHandle;
                }

                // look for mount to be removed
                auto by_pimp_it = by_pimp.find( impl );
                assert( by_pimp.end() != by_pimp_it );

                // initialize collection of mounts to be removed
                std::vector< decltype( by_pimp_it ) > to_remove;
                to_remove.reserve( mounts_.size() );
                to_remove.push_back( by_pimp_it );
                auto children_gathered = to_remove.begin();

                // all children gathered
                while ( to_remove.end() != children_gathered )
                {
                    // get mount PIMP
                    auto parent = ( *children_gathered++ )->pimp_;

                    // select children mounts
                    auto[ children_it, children_end ] = by_parent.equal_range( parent );

                    // and mark them to remove
                    while ( children_it != children_end )
                    {
                        by_pimp_it = mounts_.project< mount_t::by_pimp >( children_it++ );
                        assert( by_pimp.end() != by_pimp_it );

                        to_remove.push_back( by_pimp_it );
                    }
                }

                // if there are more than 1 mount to be removed
                if ( to_remove.size() > 1 && !force )
                {
                    // we have children mouns
                    return RetCode::HasDependentMounts;
                }

                // remove all the mounts
                for ( auto & removing_it : to_remove )
                {
                    by_pimp.erase( removing_it );
                }

                return RetCode::Ok;
            }
            catch ( ... )
            {
                return UnknownError;
            }
        }
    };
}

#endif
