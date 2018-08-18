#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


#include <atomic>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <boost/thread/shared_mutex.hpp>


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
        using NodeUid = typename PhysicalStorage::NodeUid;

        static constexpr auto RootNodeUid = PhysicalStorage::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalStorage::InvalidNodeUid;

        using PathLock = typename PathLocker::PathLock;


    private:

        //
        // manage volume access mode between shared ( regular operations ) and exclusive ( cleanup )
        //
        mutable boost::shared_mutex clean_lock_;


        //
        // filters incoming path for existance
        //
        Bloom filter_;


        //
        // locks mounted path
        //
        PathLocker path_locker_;


        //
        // let us to lock a node temporary with specified access type. Since collisions are possible
        // we cannot get exclusive lock over a mutex during search cuz that may cause deadlock, so we
        // need upgradable lock. That is the reason to use boost::shared_mutex 
        //
        static constexpr size_t node_locker_size = 41; // 42 is the answer but it's not prime to be good hasher
        std::array< boost::shared_mutex, node_locker_size > node_locker_;
        auto & get_node_locker( NodeUid uid ) { return node_locker_[ uid / node_locker_size ]; }


        auto find_key( NodeUid uid, const Key & key )
        {
            using namespace std;

            // get shared lock over uid
            boost::shared_lock lock{ get_node_locker( uid ) };

            while ( true )
            {
                // get node by uid
                b_tree_node_p node; // storage::get()

                // try find the key
                auto f = node->find( key );

                // if key found
                if ( auto pos = get< diffptr_t >( f ); pos != b_tree_node::Npos )
                {
                    return { lock, node, pos };
                }
                // key does not exists
                else if ( ( uid = get< NodeUid >( f ) ) == InvalidNodeUid )
                {
                    return { Lock{}, b_tree_node_p{}, b_tree_node::Npos };
                }
                else
                {
                    // that is here collision would be possible if we used non-upgdarable std::shared_lock
                    lock = move( boost::shared_lock{ get_node_locker( uid ) } );
                }
            }
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
        auto wait_and_do_it( const execution_connector & in, execution_connector & out, const F & f ) noexcept
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
                        decltype( f( ) ) result{};
                        std::get< RetCode >( result ) = RetCode::Ok;
                        return result;
                    }

                    // if we've been granted to perform operation
                    if ( in_do_it.load( memory_order_acquire ) )
                    {
                        // run operation
                        auto result = f( );

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
                    this_thread::sleep_for( chrono::nanoseconds::min() );
                }
            }
            catch ( ... )
            {
                // something terrible happened
            }

            // something terrible happened - cancel further operations...
            out_cancel.store( true, memory_order_release );

            // ...and return error
            decltype( f( ) ) result{};
            std::get< RetCode >( result ) = RetCode::UnknownError;
            return result;
        }


    public:

        PhysicalVolumeImpl( ) = default;


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
        [[nodiscard]]
        std::tuple < RetCode, NodeUid, PathLock > lock_path (   NodeUid entry_node_uid,
                                                                const Key & entry_path,
                                                                const Key & relative_path,
                                                                const execution_connector & in,
                                                                execution_connector & out ) noexcept
        {
            using namespace std;

            try
            {
                // root node always exists
                if ( Key{} == entry_path  && Key::root() == relative_path )
                {
                    return wait_and_do_it( in, out, [&] {
                        return tuple{ RetCode::Ok, RootNodeUid, path_locker_.lock_node( RootNodeUid ) };
                    } );
                }
                // check if path exists
                else if ( ! filter_.test( entry_path, relative_path ) )
                {
                    return wait_and_do_it( in, out, [] { 
                        return tuple{ RetCode::NotFound, InvalidNodeUid, PathLock{} };
                    } );
                }

                return wait_and_do_it( in, out, [] {
                    return tuple{ RetCode::NotImplementedYet, InvalidNodeUid, PathLock{} };
                } );
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
        [[ nodiscard ]]
        std::tuple< RetCode > insert (   NodeUid entry_node_uid,
                                         const Key & entry_path,
                                         const Key & relative_path,
                                         const Key & subkey,
                                         Value && value,
                                         Timestamp && good_before,
                                         bool overwrite,
                                         const execution_connector & in,
                                         execution_connector & out  )
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
        [[ nodiscard ]]
        std::tuple< RetCode, Value > get(   NodeUid entry_node_uid,
                                            const Key & entry_path,
                                            const Key & relative_path,
                                            const execution_connector & in,
                                            execution_connector & out )
        {
            using namespace std;

            try
            {
                //if ( !filter_.test( entry_path, relative_path ) )
                //{
                //    return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotFound, Value{} }; } );
                //} 

                return wait_and_do_it( in, out, [] { return tuple{ RetCode::NotImplementedYet, Value{} }; } );
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
        [[nodiscard]]
        std::tuple< RetCode > erase(    NodeUid entry_node_uid,
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
