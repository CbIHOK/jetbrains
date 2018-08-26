#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


#include <atomic>
#include <thread>
#include <chrono>
#include <filesystem>
#include <tuple>
#include <utility>

#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>

#include <assert.h>


namespace jb
{
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl
    {
        friend class TestNodeLocker;
        friend class TestBloom;
        friend class TestStorageFile;

    public:

        class PathLocker;
        class Bloom;
        class StorageFile;
        class BTree;
        class BTreeCache;

        using execution_connector = std::pair< std::atomic_bool, std::atomic_bool >;

    private:

        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPointImpl = typename Storage::MountPointImpl;
        using BTreeP = typename BTree::BTreeP;
        using NodeUid = typename BTree::NodeUid;
        using PathLock = typename PathLocker::PathLock;
        using Digest = typename Bloom::Digest;
        using shared_lock = boost::upgrade_lock< boost::upgrade_mutex >;
        using exclusive_lock = boost::upgrade_to_unique_lock< boost::upgrade_mutex >;
        using BTreePath = typename BTree::BTreePath;

        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

        static constexpr auto RootNodeUid = BTree::RootNodeUid;
        static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;


        RetCode status_ = RetCode::Ok;
        StorageFile file_;
        PathLocker path_locker_;
        Bloom filter_;
        BTreeCache cache_;


        /* Navigates through the tree searching for given key and getting locks over found ones...

        preventing any changes to tjem before an operation gets completed. Also preserves path to found
        digest from the root of containing b-tree. This path may be used as a hint upon erase operation
        preventing sequental search. Also let us apply a custom action on each found subkey

        @param [in] digests - path to be found
        @param [out] locks - accumulates locks over root b-tree nodes
        @param [out] bpath - holds path in b-tree from root node of a key to a node holding found digest
        @param [in] f - custom action to be done on each found digest
        @param [in] in - incoming execution events
        @throw nothing
        */
        template < typename D, typename L, typename P, typename F >
        RetCode navigate( 
            const D & digests, 
            L & locks, 
            P & bpath, 
            const F & f, 
            const execution_connector & in ) noexcept
        {
            using namespace std;

            assert( digests.size() );
            auto digest_it = begin( digests );

            // retrieve root node
            if ( auto[ rc, btree ] = cache_.get_node( RootNodeUid ); RetCode::Ok != rc )
            {
                return rc;
            }
            else
            {
                while ( ! cancelled( in ) )
                {
                    // clear b-tree path inside a key
                    bpath.clear();

                    // search for digest from start b-tree node
                    if ( auto[ rc, found ] = btree->find_digest( *digest_it, bpath ); RetCode::Ok != rc )
                    {
                        // error
                        return rc;
                    }
                    else if ( !found )
                    {
                        // not such digest found
                        return rc;
                    }
                    else
                    {
                        assert( bpath.size() );

                        // get root of b-tree
                        if ( auto[ rc, btree_root ] = cache_.get_node( bpath.front().first ); RetCode::Ok != rc )
                        {
                            return rc;
                        }
                        else
                        {
                            // lock root b-tree node of a key and therefore prevent any changes on a path
                            assert( locks.size() < locks.capacity() );
                            locks.emplace_back( btree_root, shared_lock{ btree_root->guard() } );

                            // perform custom action on root b-tree node
                            f( btree_root );
                        }

                        // get b-tree node containing a digest
                        if ( auto[ rc, found ] = cache_.get_node( bpath.back().first ); RetCode::Ok != rc )
                        {
                            return rc;
                        }
                        else
                        {
                            // get current time in msecs from epoch
                            const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 );

                            // check if found digest expired
                            if ( found->good_before( bpath.back().second ) && found->good_before( bpath.back().second ) < now )
                            {
                                return rc;
                            }
                            // if there is nothing more to search
                            else if ( ++digest_it == end( digests ) )
                            {
                                // we've done
                                return RetCode::Ok;
                            }
                            else
                            {
                                // get uid of children container 
                                if ( auto children_uid = found->children( bpath.back().second ); InvalidNodeUid != children_uid )
                                {
                                    // load children container 
                                    if ( auto[ rc, child_btree ] = cache_.get_node( children_uid ); RetCode::Ok == rc )
                                    {
                                        // continue in depth
                                        btree = child_btree;
                                        continue;
                                    }
                                    else
                                    {
                                        return rc;
                                    }
                                }
                                else
                                {
                                    return RetCode::NotFound;
                                }
                            }
                        }
                    }
                }

                // operation has been cancelled
                return RetCode::NotFound;
            }
        }


        /* Checks if current operation is being cancelled

        @retval true if the operation is being cancelled
        @throw nothing
        */
        auto static cancelled( const execution_connector & in ) noexcept
        {
            return in.first.load( memory_order_acquire );
        }


        /* Controls execution chain

        Checks if this operation is canceled and if it is - cancels further operations. Also checks
        if this operation is granted to perform an action, performs it, and, if action succeeds,
        also cancels further operations. If the action fails, informs grant further operations to
        apply

        @tparam F - type of action
        @param [in] in - incoming execution event
        @param [out] out - outgoing execution event
        @param [in] f - action to be applied
        @retval the action result
        @throws nothing
        */
        template < typename F >
        auto static wait_and_do_it( const execution_connector & in, execution_connector & out, const F & f ) noexcept
        {
            using namespace std;

            const auto & in_cancel = in.first;
            const auto & in_do_it = in.second;
            auto & out_cancel = out.first;
            auto & out_do_it = out.second;

            try
            {
                while ( true )
                {
                    // if the operation has been canceled
                    if ( in_cancel.load( memory_order_acquire ) )
                    {
                        // cancel further operations
                        out_cancel.store( true, memory_order_release );

                        // return successful status
                        decltype( f() ) result{};
                        std::get< RetCode >( result ) = RetCode::Ok;
                        return result;
                    }

                    // if we've been granted to perform operation
                    if ( in_do_it.load( memory_order_acquire ) )
                    {
                        // run operation
                        auto result = f();

                        // if operation supplied sucessfully
                        if ( RetCode::Ok == std::get< RetCode >( result ) )
                        {
                            // cancel further operations
                            out_cancel.store( true, memory_order_release );
                        }
                        else
                        {
                            // grant further operations
                            out_do_it.store( true, memory_order_release );
                        }

                        // return operation result
                        return result;
                    }

                    // fall asleep for a while: we just lost the time slice and move the thread in the end of scheduler queue
                    this_thread::sleep_for( std::chrono::nanoseconds::min() );
                }
            }
            catch ( ... )
            {
                // something terrible happened
            }

            // something terrible happened - cancel further operations...
            out_cancel.store( true, memory_order_release );

            // ...and return error
            decltype( f() ) result{};
            std::get< RetCode >( result ) = RetCode::UnknownError;
            return result;
        }


    public:

        explicit PhysicalVolumeImpl( const std::filesystem::path & path, bool create ) try
            : file_{ path, create }
            , filter_( &file_ )
            , cache_( &file_ )
        {
            status_ = std::max( status_, path_locker_.status() );
            status_ = std::max( status_, file_.status() );
            status_ = std::max( status_, filter_.status() );
            status_ = std::max( status_, cache_.status() );
        }
        catch ( ... )
        {
            status_ = RetCode::UnknownError;
        }


        /** Locks specified path due to a mounting operation

        @param [in] entry_node_uid - UID of entry that shall be used as search entry
        @param [in] entry_path - physical path of search entry
        @param [in] relative_path - path to be locked
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @retval NodeUid - UID of a node at the path
        @retval PathLock - lock over all nodes on the path begining from entry path
        */
        [[ nodiscard ]]
        std::tuple < RetCode, NodeUid, PathLock > lock_path(
            NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out ) noexcept
        {
            using namespace std;

            try
            {
                static_vector< Digest, MaxTreeDepth > digests;

                if ( auto[ rc, may_present ] = filter_.test( entry_path, relative_path, digests ); RetCode::Ok != rc )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ rc, InvalidNodeUid, PathLock{} }; } );
                }
                else if ( digests.empty() ) // root node
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::Ok, RootNodeUid, PathLock{} }; } );
                }
                else
                {
                    static_vector< pair< BTreeP, shared_lock >, MaxTreeDepth > locks;
                    BTreePath bpath;
                    PathLock path_lock;

                    auto rc = navigate( digests, locks, bpath, [&] ( const BTreeP & p ) {
                        path_lock << path_locker_.lock( p->uid() );
                    }, in );

                    if ( RetCode::Ok == rc )
                    {
                        assert( bpath.size() );
                        auto root = bpath.front();

                        return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, root.first, move( path_lock ) }; } );
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [=] { return tuple{ rc, InvalidNodeUid, PathLock{} }; } );
                    }
                }
            }
            catch ( ... )
            {
            }

            return wait_and_do_it( in, out, [] { return tuple{ RetCode::UnknownError, InvalidNodeUid, PathLock{} }; } );
        }


        /** Inserts subnode of a given name with given value and expidation timemark as specified

        path. If the subnode already exists the behavior depends on ovwewrite flag

        @param [in] entry_node_uid - UID of entry that shall be used as search entry
        @param [in] entry_path - physical path of search entry
        @param [in] relative_path - path of item to be erased relatively to entry_path_
        @param [in] subkey - name of subkey to be inserted
        @param [in] value - value to be assigned to subkey
        @param [in] good_before - expiration timemark
        @param [in] overwrite - overwrite existing node
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        */
        [[ nodiscard ]]
        std::tuple< RetCode > insert( NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const Key & subkey,
            Value && value,
            uint64_t good_before,
            bool overwrite,
            const execution_connector & in,
            execution_connector & out )
        {
            using namespace std;

            try
            {
                static_vector< Digest, MaxTreeDepth > digests;

                if ( auto[ rc, may_present ] = filter_.test( entry_path, relative_path, digests ); RetCode::Ok != rc )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ rc }; } );
                }
                else if ( digests.size() + 1 >= MaxTreeDepth )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::MaxTreeDepthExceeded }; } );
                }
                else
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                static_vector< pair< BTreeP, shared_lock >, MaxTreeDepth > locks;
                BTreePath bpath;

                if ( auto rc = navigate( digests, locks, bpath, [] ( auto ) {}, in ); RetCode::Ok == rc )
                {
                    return wait_and_do_it( in, out, [&] {
                        
                        Digest digest = Bloom::generate_digest( locks.size() + 1, subkey );

                        // get exclusive lock over the key
                        assert( locks.size() );
                        exclusive_lock e{ locks.back().second };

                        assert( bpath.size() );
                        auto target = bpath.back(); bpath.pop_back();

                        if ( auto[ rc, node ] = cache_.get_node( target.first ); RetCode::Ok == rc )
                        {
                            return tuple{ node->insert( target.second, bpath, digest, move( value ), good_before, overwrite ) };
                        }
                        else
                        {
                            return tuple{ rc };
                        }
                    } );
                }
                else
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ rc }; } );
                }
            }
            catch ( ... )
            {

            }

            return wait_and_do_it( in, out, [] { return tuple{ RetCode::UnknownError }; } );
        }


        /** Provides value of specified node

        @param [in] entry_node_uid_ - UID of entry that shall be used as search entry
        @param [in] entry_path_ - physical path of search entry
        @param [in] relative_path - path of item to be retrieved
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @retvel Value - if operation succeeded contains the value
        */
        [ [ nodiscard ] ]
        std::tuple< RetCode, Value > get(
            NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out )
        {
            using namespace std;

            try
            {
                static_vector< Digest, MaxTreeDepth > digests;

                if ( auto[ rc, may_present ] = filter_.test( entry_path, relative_path, digests ); RetCode::Ok != rc )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ rc, Value{} }; } );
                }
                else if ( ! may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound, Value{} }; } );
                }
                else
                {
                    static_vector< pair< BTreeP, shared_lock >, MaxTreeDepth > locks;
                    BTreePath bpath;

                    if ( auto rc = navigate( digests, locks, bpath, [] ( auto ) {}, in ); RetCode::Ok == rc )
                    {
                        assert( bpath.size() );
                        auto target = bpath.back(); bpath.pop_back();
                        
                        if ( auto[ rc, node ] = cache_.get_node( target.first ); RetCode::Ok == rc )
                        {
                            return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, node->value( target.second ) }; } );
                        }
                        else
                        {
                            return wait_and_do_it( in, out, [&] { return tuple{ rc, Value{} }; } );
                        }
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [&] { return tuple{ rc, Value{} }; } );
                    }
                }
            }
            catch ( ... )
            {

            }

            return wait_and_do_it( in, out, [] { return tuple{ RetCode::UnknownError, Value{} }; } );
        }


        /** Performs erasing of a given node at physical level simply marking it as erased

        Physical erasing of related data and releasing of allocated space in physical storage will
        be done upon cleanup routine

        @param [in] entry_node_uid_ - UID of entry that shall be used as search entry
        @param [in] entry_path_ - physical path of search entry
        @param [in] relative_path - path of item to be erased relatively to entry_path_
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        */
        [ [ nodiscard ] ]
        std::tuple< RetCode > erase( NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out ) noexcept
        {
            using namespace std;

            try
            {
                static_vector< Digest, MaxTreeDepth > digests;

                if ( auto[ ret, may_present ] = filter_.test( entry_path, relative_path, digests ); RetCode::Ok != ret )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ ret }; } );
                }
                else if ( !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotYetImplemented }; } );
            }
            catch ( ... )
            {

            }

            return wait_and_do_it( in, out, [] { return tuple{ RetCode::UnknownError }; } );
        }
    };
}


#include "path_locker.h"
#include "storage_file.h"
#include "bloom.h"
#include "b_tree.h"
#include "b_tree_cache.h"


#endif
