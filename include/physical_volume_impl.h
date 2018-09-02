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
        using DigestPath = typename Bloom::DigestPath;
        
        using storage_file_error = typename StorageFile::storage_file_error;
        using btree_error = typename BTree::btree_error;
        using bloom_error = typename Bloom::bloom_error;

        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

        static constexpr auto RootNodeUid = BTree::RootNodeUid;
        static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;

        using NodeLock = static_vector< shared_lock, MaxTreeDepth >;

        std::atomic< RetCode > status_ = RetCode::Ok;
        StorageFile file_;
        PathLocker path_locker_;
        Bloom filter_;
        BTreeCache cache_;

        auto set_status( RetCode status ) noexcept
        {
            auto ok = RetCode::Ok;
            status_.compare_exchange_weak( ok, status, std::memory_order_acq_rel, std::memory_order_relaxed );
        }


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
        auto navigate( 
            const D & digests, 
            L & locks, 
            P & bpath, 
            const F & f, 
            const execution_connector & in ) noexcept
        {
            using namespace std;

            if ( !digests.size() ) return true;

            auto digest_it = begin( digests );

            // retrieve root node
            auto node = cache_.get_node( RootNodeUid );

            while ( !cancelled( in ) )
            {
                // get lock over node
                shared_lock lock{ node->guard() };
                locks.push_back( std::move( lock ) );

                // clear path to digest
                bpath.clear();

                // search for a digest in current node
                auto found = node->find_digest( *digest_it, bpath );
                digest_it++;

                // digest not found
                if ( !found ) return false;

                // get node containing the digest
                node = cache_.get_node( bpath.back().first );
                auto digest_pos = bpath.back().second;

                // get digest's expiration
                auto expiration_time = node->good_before( digest_pos );
                const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 );

                // if digest expired - ignore it
                if ( expiration_time && expiration_time < now ) return false;

                // run an action on node
                f( node );

                // if node is the target
                if ( digest_it == end( digests ) ) return true;

                // get uid of child entry b-tree node
                auto child_uid = node->children( digest_pos );

                // load root of children collection and continue search
                node = cache_.get_node( child_uid );
            }

            return false;
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

        explicit PhysicalVolumeImpl( const std::filesystem::path & path ) try
            : file_{ path, true }
            , filter_( file_ )
            , cache_( file_ )
        {
            if ( RetCode::Ok != file_.status() )
            {
                set_status( file_.status() );
            }
            else if ( RetCode::Ok != cache_.status() )
            {
                set_status( cache_.status() );
            }
            else if ( RetCode::Ok != filter_.status() )
            {
                set_status( filter_.status() );
            }

            if ( file_.newly_created() )
            {
                BTree root( file_, cache_ );
                auto t = file_.open_transaction();
                root.save( t );
                t.commit();
            }
        }
        catch ( ... )
        {
            set_status( RetCode::UnknownError );
        }


        [ [ nodiscard ] ]
        auto status() const noexcept { return status_.load( std::memory_order_acquire ); }


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
                DigestPath digests;
                BTreePath bpath;
                NodeLock locks;
                PathLock path_lock;

                if ( auto may_present = filter_.test( entry_path, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ RetCode::NotFound, InvalidNodeUid, PathLock{} }; } );
                }
                else if ( digests.empty() ) // root node
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::Ok, RootNodeUid, PathLock{} }; } );
                }
                else
                {
                    auto found = navigate( digests, locks, bpath, [&] ( const BTreeP & p ) {
                        path_lock << path_locker_.lock( p->uid() );
                    }, in );

                    if ( found )
                    {
                        return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, BTree::RootNodeUid, move( path_lock ) }; } );
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [=] { return tuple{ RetCode::NotFound, InvalidNodeUid, PathLock{} }; } );
                    }
                }
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, PathLock{} }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, PathLock{} }; } );
            }
            catch ( const bloom_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, PathLock{} }; } );
            }
            catch ( ... )
            {
                set_status( RetCode::UnknownError );
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::UnknownError, InvalidNodeUid, PathLock{} }; } );
            }
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
                DigestPath digests;
                BTreePath bpath;
                NodeLock locks;

                if ( auto may_present = filter_.test( entry_path, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                auto target_node = cache_.get_node( RootNodeUid );

                if ( digests.size() )
                {
                    if ( auto found = navigate( digests, locks, bpath, [] ( auto ) {}, in ) )
                    {
                        assert( bpath.size() );
                        auto parent_btree = cache_.get_node( bpath.back().first );

                        {
                            assert( locks.size() );
                            exclusive_lock e{ locks.back() };

                            parent_btree->deploy_children_btree( bpath.back().second );
                        }

                        target_node = cache_.get_node( parent_btree->children( bpath.back().second ) );
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                    }
                }
                else if ( digests.size() + 1 >= MaxTreeDepth )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::MaxTreeDepthExceeded }; } );
                }

                {
                    locks.push_back( shared_lock{ target_node->guard() } );

                    Digest digest = Bloom::generate_digest( digests.size() + 1, subkey );

                    BTreePath bpath;
                    target_node->find_digest( digest, bpath );

                    assert( bpath.size() );
                    auto target_btree = cache_.get_node( bpath.back().first );

                    wait_and_do_it( in, out, [&] {

                        {
                            assert( locks.size() );
                            exclusive_lock e{ locks.back() };

                            BTree::Pos target_pos = bpath.back().second; bpath.pop_back();
                            target_btree->insert( target_pos, bpath, digest, value, good_before, overwrite );
                        }

                        filter_.add_digest( digest );

                        return tuple{ RetCode::Ok };
                    } );
                }
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const bloom_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( ... )
            {
                set_status( RetCode::UnknownError );
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::UnknownError }; } );
            }
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
                DigestPath digests;
                BTreePath bpath;
                NodeLock locks;

                if ( auto may_present = filter_.test( entry_path, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound, Value{} }; } );
                }
                else if ( auto found = navigate( digests, locks, bpath, [] ( auto ) {}, in ); !found )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::NotFound, Value{} }; } );
                }
                else
                {
                    auto node_uid = bpath.back().first;
                    auto pos = bpath.back().second;

                    auto node = cache_.get_node( node_uid );

                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, node->value( pos ) }; } );
                }
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), Value{} }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), Value{} }; } );
            }
            catch ( const bloom_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), Value{} }; } );
            }
            catch ( ... )
            {
                set_status( RetCode::UnknownError );
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::UnknownError, Value{} }; } );
            }
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
                DigestPath digests;
                BTreePath bpath;
                NodeLock locks;

                if ( auto may_present = filter_.test( entry_path, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }
                else if ( auto found = navigate( digests, locks, bpath, [] ( auto ) {}, in ); !found )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::NotFound }; } );
                }
                else
                {
                    return wait_and_do_it( in, out, [&] { 

                        // get exclusive lock over the key
                        exclusive_lock e{ locks.back() };

                        auto node_uid = bpath.back().first;
                        auto pos = bpath.back().second;

                        auto node = cache_.get_node( node_uid );
                        node->erase( pos, bpath );

                        return tuple{ RetCode::Ok };

                    } );
                }
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const bloom_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( ... )
            {
                set_status( RetCode::UnknownError );
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::UnknownError }; } );
            }
        }
    };
}


#include "path_locker.h"
#include "storage_file.h"
#include "bloom.h"
#include "b_tree.h"
#include "b_tree_cache.h"


#endif
