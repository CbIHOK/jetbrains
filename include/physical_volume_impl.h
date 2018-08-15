#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


#include <atomic>
#include <thread>
#include <chrono>


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
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPointImpl = typename Storage::MountPointImpl;

        class NodeLocker;
        class Bloom;

    public:

        typedef size_t NodeUid;
        using NodeLock = typename NodeLocker::NodeLock;
        
        PhysicalVolumeImpl( ) = default;

        using execution_connector = std::pair< std::atomic_bool, std::atomic_bool >;


        [[nodiscard]]
        static auto check_for_cancel( const execution_connector & in, execution_connector & out ) noexcept
        {
            using namespace std;

            if ( auto cancel = incoming.cancel_.load( memory_order_acquire ) )
            {
                outgoing.cancel_.store( true, memory_order_release );

                return true;
            }

            return false;;
        }


        [[nodiscard]]
        static auto check_for_doit( const execution_connector & in, execution_connector & out ) noexcept
        {
            using namespace std;
            
            const atomic_bool & in_do_it = in.second;
            atomic_bool & out_cancel = out.first;

            if ( auto doit = in_do_it.load( memory_order_acquire ) )
            {
                out_cancel.store( true, memory_order_release );
                return true;
            }

            return false;
        }


        [[nodiscard]]
        std::tuple< RetCode, NodeUid, NodeLock > lock_path( 
            NodeUid entry_node_uid_,
            const Key & entry_path_,
            const Key & relative_path,
            const execution_connector & in,
            execution_connector & out )
        {
            using namespace std;

            while ( !check_for_doit( in, out ) )
            {
                this_thread::sleep_for( 1ms );
            }

            return { RetCode::Ok, NodeUid{ 0 }, NodeLock{} };
        }


    private:

        NodeLock locks_;
    };
}


#include "node_locker.h"
#include "bloom.h"


#endif
