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
    /** Implementation of Physical Volume

    The class is responsible for creating physical volume infrastructure and for operations over
    keys: locking, inserting, getting, and erasing, but mostly for navigation by key tree

    @tparam Policies - global settings
    */
    template < typename Policies >
    class PhysicalVolumeImpl
    {
        friend class TestNodeLocker;
        friend class TestBloom;

    public:

        //
        // internal classes
        //
        class PathLocker;
        class Bloom;
        class StorageFile;
        class BTree;
        class BTreeCache;

        //
        // export few aliases
        //
        static constexpr auto RootNodeUid = BTree::RootNodeUid;
        static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;


        /** Represents execution signals: CANCELLED signal and ALLOWED TO APPLY signal
        */
        struct execution_chain : std::atomic_uint32_t
        {
            enum{ st_not_defined = 0, st_cancelled, st_allowed };

            execution_chain() : std::atomic_uint32_t( st_not_defined ) {}

            void cancel() noexcept
            {
                store( st_cancelled, std::memory_order::memory_order_relaxed );
            }

            void allow() noexcept
            {
                store( st_allowed, std::memory_order::memory_order_relaxed );
            }
            
            bool cancelled() const noexcept
            {
                return ( st_cancelled = load( std::memory_order::memory_order_relaxed ) );
            }

            void wait_and_let_further_go( execution_chain * further ) const noexcept
            {
                for ( size_t try_count = 1;; ++try_count )
                {
                    const auto value = load( std::memory_order::memory_order_relaxed );

                    if ( st_cancelled == value && further )
                    {
                        further->cancel();
                        break;
                    }
                    else if ( st_allowed == value && further )
                    {
                        further->allow();
                        break;
                    }
                    else if ( try_count % 0xFFFF == 0 )
                    {
                        std::this_thread::yield;
                    }
                }
            }

            bool wait_and_cancel_further( execution_chain * further ) const noexcept
            {
                for ( size_t try_count = 1;; ++try_count )
                {
                    const auto value = load( std::memory_order::memory_order_relaxed );

                    if ( st_cancelled == value && st_allowed == value )
                    {
                        if ( further )
                        {
                            further->cancel();
                        }
                        return ( st_allowed == value );
                    }
                    else if ( try_count % 0xFFFF == 0 )
                    {
                        std::this_thread::yield;
                    }
                }
            }
        };


    private:

        //
        // importing aliases
        //
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
        using btree_cache_error = typename BTreeCache::btree_cache_error;

        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;


        /* Represents locking over a key
        */
        using KeyLock = static_vector< shared_lock, MaxTreeDepth >;


        //
        // data members
        //
        RetCode status_ = RetCode::Ok;
        std::unique_ptr< StorageFile > file_;
        std::unique_ptr< PathLocker > path_locker_;
        std::unique_ptr< Bloom > filter_;
        std::unique_ptr< BTreeCache > cache_;
        size_t priority_;


        /* Throws std::logic_error if a condition failed and immediately dies on noexcept guard

        calling terminate() handler. That gives the ability to collect crash dump for futher
        analysis

        @param [in] condition - condition to be checked
        @param [in] what - text message to be assigned to an error
        @throw nothing
        */
        auto static throw_logic_error( bool condition, const char * what = "" ) noexcept
        {
            if ( !condition ) throw std::logic_error( what );
        }


        /* Navigates through the tree searching for given key and getting locks over found ones...

        preventing any changes to tjem before an operation gets completed. Also preserves path to found
        digest from the root of containing b-tree. This path may be used as a hint upon erase operation
        preventing sequental search. Also let us apply a custom action on each found subkey

        @param [in] entry_node - start point for navigating
        @param [in] digests - path to be found
        @param [out] locks - accumulates locks over visited nodes
        @param [out] bpath - holds path in b-tree from root node of a key to a node holding found digest
        @param [in] f - custom action to be done on each found digest
        @param [in] in - incoming execution events
        @throw nothing
        */
        template < typename D, typename L, typename P, typename F >
        auto navigate( 
            BTreeP entry_node,
            const D & digests, 
            L & locks, 
            P & bpath, 
            const F & f, 
            const execution_chain & in ) noexcept
        {
            using namespace std;

            // if there is something to navigate?
            if ( !digests.size() ) return true;

            auto node = entry_node;
            auto digest_it = begin( digests );

            while ( !cancelled( in ) )
            {
                // get lock over node
                shared_lock lock{ node->guard() };
                locks.push_back( std::move( lock ) );

                // run an action on the root b-tree node to lock it for removing, there is not a
                // reason to run it on entry key, cuz it's either / that cannot be removed or it's
                // already mounted, i.e. is already locked
                f( node );

                // clear path to digest
                bpath.clear();

                // search for a digest in current node
                auto found = node->find_digest( *digest_it, bpath );
                digest_it++;

                // digest not found
                if ( !found ) return false;

                // get node containing the digest
                node = cache_->get_node( bpath.back().first );
                auto digest_pos = bpath.back().second;

                // get digest's expiration
                auto expiration_time = node->good_before( digest_pos );
                const uint64_t now = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds( 1 );

                // if digest expired - ignore it
                if ( expiration_time && expiration_time < now ) return false;

                // if node is the target
                if ( digest_it == end( digests ) ) return true;

                // get uid of child entry b-tree node
                auto child_uid = node->children( digest_pos );

                // if children present?
                if ( InvalidNodeUid == child_uid ) return false;

                // load root b-tree node of children collection and continue search
                node = cache_->get_node( child_uid );
            }

            return false;
        }


        /* Checks if current operation is being cancelled

        @retval true if the operation is being cancelled
        @throw nothing
        */
        auto static cancelled( const execution_chain & in ) noexcept
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
        @throws may throw everything what F does
        */
        template < typename F >
        auto static wait_and_do_it( const execution_chain & in, execution_chain & out, const F & f )
        {
            using namespace std;

            const auto & in_cancel = in.first;
            const auto & in_do_it = in.second;
            auto & out_cancel = out.first;
            auto & out_do_it = out.second;

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

            // something terrible happened - cancel further operations...
            out_cancel.store( true, memory_order_release );

            // ...and return error
            decltype( f() ) result{};
            std::get< RetCode >( result ) = RetCode::UnknownError;
            return result;
        }


    public:

        /** Explicit constructor 

        Allocates all necessary infrastructure, including creating root directory for new files

        @param path - path to physical storage
        @throw nothing
        */
        explicit PhysicalVolumeImpl( const std::filesystem::path & path, size_t priority = 0 ) noexcept try : priority_( priority )
        {
            // initialize file storage
            file_ = std::make_unique< StorageFile >( path );
            if ( auto file_status = file_->status(); RetCode::Ok != file_status )
            {
                status_ = file_status;
                return;
            }

            // initialize B-tree cache
            cache_ = std::make_unique< BTreeCache >( *file_ );
            if ( auto cache_status = cache_->status(); RetCode::Ok != cache_status )
            {
                status_ = cache_status;
                return;
            }

            // initialize Bloom filter
            filter_ = std::make_unique< Bloom >( *file_ );
            if ( auto filter_status = filter_->status(); RetCode::Ok != filter_status )
            {
                status_ = filter_status;
                return;
            }

            // initialize path locker
            path_locker_ = std::make_unique< PathLocker >();
            if ( auto locker_status = path_locker_->status(); RetCode::Ok != locker_status )
            {
                status_ = locker_status;
                return;
            }

            // if physical file has been just created
            if ( file_->newly_created() )
            {
                // deploy root b-tree
                BTree root( *file_, *cache_ );
                auto t = file_->open_transaction();
                root.save( t );
                t.commit();
            }
        }
        catch ( const std::bad_alloc & )
        {
            status_ = RetCode::InsufficientMemory;
        }


        /** Provides physical volume status

        @retval RetCode - status
        @throw nothing
        */
        [[ nodiscard ]]
        auto status() const noexcept { return status_; }


        /** Locks specified path due to a mounting operation

        @param [in] entry_node_uid - mount point node
        @param [in] entry_node_level - level of mount point in a tree of physical keys
        @param [in] relative_path - path to be locked
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @retval NodeUid - UID of a node at the path
        @retval PathLock - lock over all nodes on the path begining from entry path
        */
        [[ nodiscard ]]
        std::tuple < RetCode, NodeUid, size_t, PathLock > lock_path(
            NodeUid entry_key_uid,
            size_t entry_key_level,
            const Key & relative_path,
            const execution_chain & in,
            execution_chain & out ) noexcept
        {
            using namespace std;

            try
            {
                DigestPath digests;
                BTreePath bpath;
                KeyLock locks;
                PathLock path_lock;

                // check if the volume is Ok
                throw_logic_error( RetCode::Ok == status_, "Invalid physical volume" );

                // check if relative path may present
                if ( auto may_present = filter_->test( entry_key_level, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [=] { return tuple{ RetCode::NotFound, InvalidNodeUid, 0, PathLock{} }; } );
                }
                // if entry node is being mounted?
                else if ( digests.empty() )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, entry_key_uid, 0, PathLock{} }; } );
                }
                else
                {
                    // get entry node
                    auto entry_node = cache_->get_node( entry_key_uid );

                    // navigate through the tree and get locks over visited nodes
                    auto found = navigate( entry_node, digests, locks, bpath, [&] ( const BTreeP & p ) {
                        path_lock << path_locker_->lock( p->uid() );
                    }, in );

                    if ( found )
                    {
                        assert( bpath.size() );
                        return wait_and_do_it( in, out, [&] { return tuple{ RetCode::Ok, bpath.front().first, entry_key_level + digests.size(), move( path_lock ) }; } );
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [=] { return tuple{ RetCode::NotFound, InvalidNodeUid, 0, PathLock{} }; } );
                    }
                }
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, 0, PathLock{} }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, 0, PathLock{} }; } );
            }
            catch ( const btree_cache_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), InvalidNodeUid, 0, PathLock{} }; } );
            }
            catch ( const std::bad_alloc & )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::InsufficientMemory, InvalidNodeUid, 0, PathLock{} }; } );
            }
        }


        /** Inserts subnode of a given name with given value and expidation timemark as specified

        path. If the subnode already exists the behavior depends on ovwewrite flag

        @param [in] entry_node_uid - mount point node
        @param [in] entry_node_level - level of mount point in a tree of physical keys
        @param [in] relative_path - path of item to be erased relatively to entry_path_
        @param [in] subkey - name of subkey to be inserted
        @param [in] value - value to be assigned to subkey
        @param [in] good_before - expiration timemark
        @param [in] overwrite - overwrite existing node
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode > insert(
            NodeUid entry_key_uid,
            size_t entry_key_level,
            const Key & relative_path,
            const Key & subkey,
            const Value & value,
            uint64_t good_before,
            bool overwrite,
            const execution_chain & in,
            execution_chain & out ) noexcept
        {
            using namespace std;

            try
            {
                DigestPath digests;
                BTreePath bpath;
                KeyLock locks;

                // check if the volume is Ok
                throw_logic_error( RetCode::Ok == status_, "Invalid physical volume" );

                // chech if target path exists
                if ( auto may_present = filter_->test( entry_key_level, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                // set target to entry node
                auto entry_node = cache_->get_node( entry_key_uid );
                auto target_node = entry_node;

                // if targte is not root
                if ( digests.size() )
                {
                    // find target node and ensure that their children b-tree exisis
                    if ( auto found = navigate( entry_node, digests, locks, bpath, [] ( auto ) {}, in ) )
                    {
                        assert( bpath.size() );
                        auto parent_btree = cache_->get_node( bpath.back().first );

                        {
                            // exclusively lock target node
                            assert( locks.size() );
                            exclusive_lock e{ locks.back() };

                            // deploy children b-tree
                            parent_btree->deploy_children_btree( bpath.back().second );
                        }

                        // set just deployed children b-tree as target
                        target_node = cache_->get_node( parent_btree->children( bpath.back().second ) );
                    }
                    else
                    {
                        return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                    }
                }
                else
                {
                    // get lock over root
                    locks.push_back( shared_lock{ target_node->guard() } );
                }

                // if inserting new subkey causes exceeding maximum tree depth?
                if ( digests.size() + 1 >= MaxTreeDepth )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::MaxTreeDepthExceeded }; } );
                }

                //insertion
                return wait_and_do_it( in, out, [&] {

                    // generate digest for the subkey
                    Digest digest = Bloom::generate_digest( digests.size() + 1, subkey );

                    // find target b-tree node to insertion
                    BTreePath bpath;
                    target_node->find_digest( digest, bpath );

                    // obtain target b-tree node
                    assert( bpath.size() );
                    auto target_btree = cache_->get_node( bpath.back().first );

                    {
                        // get exclusive lock over target key
                        assert( locks.size() );
                        exclusive_lock e{ locks.back() };

                        // and insert new subkey
                        BTree::Pos target_pos = bpath.back().second; bpath.pop_back();
                        target_btree->insert( target_pos, bpath, digest, value, good_before, overwrite );
                    }

                    // force filter to respect new digest
                    filter_->add_digest( digest );

                    return tuple{ RetCode::Ok };
                } );
            }
            catch ( const storage_file_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const btree_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const btree_cache_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const std::bad_alloc & )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::InsufficientMemory }; } );
            }
        }


        /** Provides value of specified node

        @param [in] entry_node_uid - mount point node
        @param [in] entry_node_level - level of mount point in a tree of physical keys
        @param [in] relative_path - path of item to be retrieved
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @retval Value - if operation succeeded contains the value
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode, Value > get(
            NodeUid entry_key_uid,
            size_t entry_key_level,
            const Key & relative_path,
            const execution_chain & in,
            execution_chain & out ) noexcept
        {
            using namespace std;

            try
            {
                DigestPath digests;
                BTreePath bpath;
                KeyLock locks;

                // check if the volume is Ok
                throw_logic_error( RetCode::Ok == status_, "Invalid physical volume" );

                // get entry node
                auto entry_node = cache_->get_node( entry_key_uid );

                // check that key presents
                if ( auto may_present = filter_->test( entry_key_level, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound, Value{} }; } );
                }
                // find the key
                else if ( auto found = navigate( entry_node, digests, locks, bpath, [] ( auto ) {}, in ); !found )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::NotFound, Value{} }; } );
                }
                else
                {
                    // get target b-tree
                    auto node = cache_->get_node( bpath.back().first );

                    auto pos = bpath.back().second; bpath.pop_back();

                    // return value
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
            catch ( const btree_cache_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code(), Value{} }; } );
            }
            catch ( const std::bad_alloc & )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::InsufficientMemory, Value{} }; } );
            }
        }


        /** Performs erasing of a given node at physical level simply marking it as erased

        Physical erasing of related data and releasing of allocated space in physical storage will
        be done upon cleanup routine

        @param [in] entry_node_uid - mount point node
        @param [in] entry_node_level - level of mount point in a tree of physical keys
        @param [in] relative_path - path of item to be erased relatively to entry_path_
        @param [in] in - incoming execution events
        @param [out] in - outgoing execution events
        @retval RetCode - status of operation
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode > erase(
            NodeUid entry_node_uid,
            size_t entry_node_level,
            const Key & relative_path,
            const execution_chain & in,
            execution_chain & out ) noexcept
        {
            using namespace std;

            try
            {
                DigestPath digests;
                BTreePath bpath;
                KeyLock locks;

                // check if the volume is Ok
                throw_logic_error( RetCode::Ok == status_, "Invalid physical volume" );

                // get entry node
                auto entry_node = cache_->get_node( entry_node_uid );

                if ( auto may_present = filter_->test( entry_node_level, relative_path, digests ); !may_present )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }
                else if ( digests.size() == 0 )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::InvalidLogicalPath }; } );
                }
                else if ( auto found = navigate( entry_node, digests, locks, bpath, [] ( auto ) {}, in ); !found )
                {
                    return wait_and_do_it( in, out, [&] { return tuple{ RetCode::NotFound }; } );
                }
                else
                {
                    // check if the key is locked by mount
                    assert( bpath.size() );
                    if ( !path_locker_->is_removable( bpath.front().first ) )
                    {
                        return wait_and_do_it( in, out, [&] { return tuple{ RetCode::PathLocked }; } );
                    }

                    return wait_and_do_it( in, out, [&] {

                        // get b-tree node containing the element
                        auto node_uid = bpath.back().first;
                        auto pos = bpath.back().second;
                        bpath.pop_back();
                        auto node = cache_->get_node( node_uid );

                        // get exclusive lock over the key
                        exclusive_lock e{ locks.back() };

                        // ... and erase the element
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
            catch ( const btree_cache_error & e )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ e.code() }; } );
            }
            catch ( const std::bad_alloc & )
            {
                return wait_and_do_it( in, out, [&] { return tuple{ RetCode::InsufficientMemory }; } );
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
