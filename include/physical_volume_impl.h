#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


#include <atomic>
#include <thread>
#include <chrono>
#include <filesystem>
#include <shared_mutex>
#include <boost/container/static_vector.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>


class TestNodeLocker;
class TestBloom;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl
    {
        friend class TestNodeLocker;
        friend class TestBloom;

        class PathLocker;
        class Bloom;
        class PhysicalStorage;

    public:

        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPointImpl = typename Storage::MountPointImpl;
        using execution_connector = std::pair< std::atomic_bool, std::atomic_bool >;
        using BTree = typename PhysicalStorage::BTree;
        using BTreeP = typename PhysicalStorage::BTreeP;
        using NodeUid = typename PhysicalStorage::NodeUid;

        static constexpr auto RootNodeUid = PhysicalStorage::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalStorage::InvalidNodeUid;
        static constexpr auto ExpectedTreeDepth = Policies::PhysicalVolumePolicy::ExpectedTreeDepth;

        using PathLock = typename PathLocker::PathLock;


    private:

        RetCode creation_status_;


        //
        // manage volume access mode between shared ( regular operations ) and exclusive ( cleanup )
        //
        std::shared_mutex cleanup_lock_;


        //
        // filters incoming path for existance
        //
        Bloom filter_;


        //
        // locks mounted path
        //
        PathLocker path_locker_;


        //
        //
        //
        PhysicalStorage physical_storage_;


        /* Search for key starting from given B-tree node

        @param [in] node - B-tree node
        @param [in] key - key to be found
        @param [in] in - incoming execution events
        @retval BTreeP - B-tree node containing the key
        @retval BTree::Pos - position of the key in B-tree node
        @throw may throw std::exception for some reason
        */
        auto find_key( BTreeP node, const Key & key, const execution_connector & in )
        {
            using namespace std;

            while ( !cancelled( in ) )
            {
                // try find the key
                auto f = node->find_key( key );

                // if key found
                if ( auto pos = std::get< 0 >( f ); pos != BTree::Npos )
                {
                    return tuple{ RetCode::Ok, node, pos };
                }
                // if there is another B-tree node for search
                else if ( auto uid = std::get< 1 >( f ); uid != InvalidNodeUid )
                {
                    // continue for next B-tree node
                    auto[ ret, next ]= physical_storage_.get_node( uid );
                    if ( ret != RetCode::Ok )
                    {
                        return tuple{ ret, BTreeP{}, BTree::Npos };
                    }
                    assert( node );

                    node = next;
                    continue;
                }
                // such key does not exist
                else
                {
                    break;
                }
            }

            return tuple{ RetCode::NotFound, BTreeP{}, BTree::Npos };
        }


        /* Navigates throght the tree searching for given key and locks found B-tree node with requested

        requested lock type

        @tparam F - type of action to be performed on each key node
        @tparam PathLockHolder - type of lock accumulator
        @param [in] uid - start B-tree node uid
        @param [in] path - relative path
        @param [in] f - action to be taken on each found node of the key
        @param [in] in - incoming execution events
        @param [out] path_lockholder - accumulates shared locks over the path to preserve it until an operation gets completed
        @retval RetCode - status of the operation
        @retval BTreeP - pointer to found B-tree node
        @retval BTree:Pos - position of the key in B-tree node
        @throw may throw std::exception for some reasons
        */
        template < typename PathLockHolder, typename F >
        auto navigate( NodeUid uid, const Key & key, const execution_connector & in, PathLockHolder & path_lockholder, const F & f )
        {
            using namespace boost;

            assert( uid != InvalidNodeUid );
            assert( key.is_path() );
            auto subkey = key;

            // get start node by uid
            auto [ ret, node ] = physical_storage_.get_node( uid );
            if ( ret != RetCode::Ok )
            {
                return tuple{ ret, BTreeP{}, BTree::Npos };
            }
            assert( node );

            // get shared lock over "/"
            if ( path_lockholder.size() < path_lockholder.capacity() )
            {
                path_lockholder.emplace_back( upgrade_lock< upgrade_mutex > { node->guard() } );
            }
            else
            {
                return tuple{ RetCode::MaxSearchDepthExceeded, BTreeP{}, BTree::Npos };
            }

            // while not cacelled
            while ( !cancelled( in ) )
            {
                // split path by the end of the 1st segment
                auto [ chunk_ok, chunk, subkey ] = subkey.split_at_head();
                assert( chunk_ok );

                // cut lead separator
                auto [ stem_ok, stem ] = chunk.cut_lead_separator();
                assert( stem_ok );

                // search for the key starting from uid
                auto [ ret, subnode, pos ] = find_key( node, stem, in );

                // if key not found
                if ( ret != RetCode::Ok )
                {
                    return tuple{ ret, BTreeP{}, BTree::Npos };
                }
                else
                {
                    assert( subnode && pos != BTree::Npos );

                    // get shared lock over found subkey
                    upgrade_lock< upgrade_mutex > subnode_shared_lock{ node->guard() };

                    // check if found key expired
                    if ( subnode->expiration( pos ) < Timestamp::clock::now() )
                    {
                        // get exclusive lock over parent node
                        upgrade_to_unique_lock< upgrade_mutex > exclusive_lock{ subnode_shared_lock };

                        // erase expired key
                        if ( auto ret = subnode->erase( pos ); RetCode::Ok == ret )
                        {
                            return tuple{ RetCode::NotFound, BTreeP{}, BTree::Npos };
                        }
                        else
                        {
                            return tuple{ ret, BTreeP{}, BTree::Npos };
                        }
                    }

                    // preserve taken lock over subnode
                    if ( path_lockholder.size() < path_lockholder.capacity() )
                    {
                        path_lockholder.emplace_back( std::move( subnode_shared_lock ) );
                    }
                    else
                    {
                        return tuple{ RetCode::MaxSearchDepthExceeded, BTreeP{}, BTree::Npos };
                    }

                    // apply action to the found key
                    f( subnode );

                    // if nothing left to search
                    if ( ! subkey.size() )
                    {
                        return tuple{ RetCode::Ok, subnode, pos };
                    }
                    else
                    {
                        // search deeper
                        node = subnode;
                        continue;
                    }
                }
            }

            return tuple{ RetCode::NotFound, BTreeP{}, BTree::Npos };
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

        PhysicalVolumeImpl( const std::filesystem::path & path ) try : physical_storage_{ path }
        {
            creation_status_ = std::max( creation_status_, path_locker_.creation_status() );
            creation_status_ = std::max( creation_status_, physical_storage_.creation_status() );
        }
        catch ( ... )
        {
            creation_status_ = RetCode::UnknownError;
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
        [ [ nodiscard ] ]
        std::tuple < RetCode, NodeUid, PathLock > lock_path( NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out ) noexcept
        {
            using namespace std;
            using namespace boost;
            using namespace boost::container;

            try
            {
                // root node always exists
                if ( Key{} == entry_path && Key::root() == relative_path )
                {
                    return wait_and_do_it( in, out, [&] {
                        return tuple{ RetCode::Ok, RootNodeUid, path_locker_.lock_node( RootNodeUid ) };
                    } );
                }
                // check if path exists
                else if ( !filter_.test( entry_path, relative_path ) )
                {
                    return wait_and_do_it( in, out, [] {
                        return tuple{ RetCode::NotFound, InvalidNodeUid, PathLock{} };
                    } );
                }

                static_vector< upgrade_lock< upgrade_mutex >, ExpectedTreeDepth > locks;
                PathLock mount_lock;
                NodeUid mount_node_uid = InvalidNodeUid;

                // navigate through to path and take lock over each node
                auto[ ret, btree, pos ] = navigate( entry_node_uid, relative_path, in, locks, [&] ( const auto & node ) {
                    assert( node );
                    mount_node_uid = node->uid();
                    mount_lock << path_locker_.lock_node( node->uid() );
                } );

                if ( RetCode::Ok == ret )
                {
                    // return path lock object 
                    return wait_and_do_it( in, out, [&] {
                        return tuple{ RetCode::NotImplementedYet, InvalidNodeUid, std::move( mount_lock ) };
                    } );
                }
            }
            catch ( ... )
            {
            }

            return wait_and_do_it( in, out, [] {
                return tuple{ RetCode::UnknownError, InvalidNodeUid, PathLock{} };
            } );
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
        [ [ nodiscard ] ]
        std::tuple< RetCode > insert( NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const Key & subkey,
            Value && value,
            Timestamp && good_before,
            bool overwrite,
            const execution_connector & in,
            execution_connector & out )
        {
            using namespace std;

            try
            {
                if ( !filter_.test( entry_path, relative_path ) )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotImplementedYet }; } );
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
        std::tuple< RetCode, Value > get( NodeUid entry_node_uid,
            const Key & entry_path,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out )
        {
            using namespace std;
            using namespace boost;
            using namespace boost::container;

            try
            {
                //if ( !filter_.test( entry_path, relative_path ) )
                //{
                //    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound, Value{} }; } );
                //} 
                
                static_vector< upgrade_lock< upgrade_mutex >, ExpectedTreeDepth > locks;

                auto[ ret, btree, pos ] = navigate( entry_node_uid, 
                    relative_path, 
                    in, 
                    locks, 
                    [&] ( [[maybe_unused]] const auto & BTreeP ) {} );

                if ( RetCode::Ok == ret )
                {
                    return wait_and_do_it( in, out, [=] {
                        auto value = btree->value( pos );
                        return tuple{ RetCode::Ok, std::move( value ) };
                    } );
                }
                else
                {
                    return wait_and_do_it( in, out, [=] {
                        return tuple{ ret, Value{} };
                    } );

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
                if ( !filter_.test( entry_path, relative_path ) )
                {
                    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound }; } );
                }

                return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotImplementedYet }; } );
            }
            catch ( ... )
            {

            }

            return wait_and_do_it( in, out, [] { return tuple{ RetCode::UnknownError }; } );
        }
    };
}


#include "path_locker.h"
#include "bloom.h"
#include "physical_storage.h"


#endif
