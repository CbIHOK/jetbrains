#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


#include <atomic>
#include <thread>
#include <chrono>
#include <shared_mutex>


class TestNodeLocker;
class TestBloom;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl
    {
        friend class TestNodeLocker;
        friend class TestBloom;

        using Storage = ::jb::Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPointImpl = typename Storage::MountPointImpl;

        class NodeLocker;
        class Bloom;

        //
        // manage volume access mode between shared ( regular operations ) and exclusive ( cleanup )
        //
        mutable std::shared_mutex volume_lock_;

    public:

        typedef unsigned long long NodeUid;
        using NodeLock = typename NodeLocker::NodeLock;
        
        PhysicalVolumeImpl( ) = default;

        using execution_connector = std::pair< std::atomic_bool, std::atomic_bool >;


        [[nodiscard]]
        static auto check_for_cancel( const execution_connector & in, execution_connector & out ) noexcept
        {
            using namespace std;

            const atomic_bool & in_cancel = in.first;
            atomic_bool & out_cancel = out.first;

            if ( in_cancel.load( memory_order_acquire ) )
            {
                out_cancel.store( true, memory_order_release );
                return true;
            }

            return false;;
        }


        [[nodiscard]]
        static auto check_for_doit( const execution_connector & in, execution_connector & out ) noexcept
        {
            using namespace std;
            
            const atomic_bool & in_cancel = in.first;
            const atomic_bool & in_do_it = in.second;
            atomic_bool & out_cancel = out.first;

            if ( in_do_it.load( memory_order_acquire ) )
            {
                out_cancel.store( true, memory_order_release );
                return true;
            }

            return false;
        }


        /** Locks volume in shared mode
        */
        [[nodiscard]]
        std::shared_lock< std::shared_mutex > get_shared_lock() const
        {
            return std::shared_lock< std::shared_mutex >( volume_lock_ );
        }


        [[nodiscard]]
        std::tuple < RetCode, NodeUid, NodeLock > lock_path (   NodeUid entry_node_uid_,
                                                                const Key & entry_path_,
                                                                const Key & relative_path,
                                                                const execution_connector & in,
                                                                execution_connector & out )
        {
            using namespace std;

            while ( true )
            {
                if ( check_for_doit( in, out ) )
                {
                    return { RetCode::Ok, NodeUid{ 0 }, NodeLock{} };
                }
                else if ( check_for_cancel( in, out ) )
                {
                    return { RetCode::Ok, NodeUid{ 0 }, NodeLock{} };

                }
                this_thread::sleep_for( 1ms );
            }
        }


        [[ nodiscard ]]
        std::tuple< RetCode > insert (   NodeUid entry_node_uid_,
                                         const Key & entry_path_,
                                         const Key & relative_path,
                                         const Key & subkey,
                                         Value && value,
                                         Timestamp && good_before,
                                         bool overwrite,
                                         const execution_connector & in,
                                         execution_connector & out  )
        {
            using namespace std;

            while ( true )
            {
                if ( check_for_doit( in, out ) )
                {
                    return { RetCode::NotImplementedYet };
                }
                else if ( check_for_cancel( in, out ) )
                {
                    return { RetCode::Ok };
                }
                this_thread::sleep_for( 1ms );
            }
        }


        [[ nodiscard ]]
        std::tuple< RetCode, Value > get(   NodeUid entry_node_uid_,
                                            const Key & entry_path_,
                                            const Key & relative_path,
                                            const execution_connector & in,
                                            execution_connector & out )
        {
            using namespace std;

            while ( true )
            {
                if ( check_for_doit( in, out ) )
                {
                    return { RetCode::NotImplementedYet, Value{} };
                }
                else if ( check_for_cancel( in, out ) )
                {
                    return { RetCode::Ok, Value{} };
                }
                this_thread::sleep_for( 1ms );
            }
        }


        [[nodiscard]]
        std::tuple< RetCode > erase(    NodeUid entry_node_uid_,
                                        const Key & entry_path_,
                                        const Key & relative_path,
                                        const execution_connector & in,
                                        execution_connector & out )
        {
            using namespace std;

            while ( true )
            {
                if ( check_for_doit( in, out ) )
                {
                    return { RetCode::NotImplementedYet };
                }
                else if ( check_for_cancel( in, out ) )
                {
                    return { RetCode::Ok };

                }
                this_thread::sleep_for( 1ms );
            }
        }
    };
}


#include "node_locker.h"
#include "bloom.h"


#endif
