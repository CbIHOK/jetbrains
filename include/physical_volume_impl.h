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

        struct execution_chain
        {
            std::atomic_bool cancel_;
            std::atomic_bool doit_;
        };


        [[nodiscard]]
        static auto check_for_cancel( const execution_chain & incoming, execution_chain & outgoing ) noexcept
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
        static auto check_for_doit( const execution_chain & incoming, execution_chain & outgoing ) noexcept
        {
            if ( auto doit = incoming.doit_.load( memory_order_acquire ) )
            {
                outgoing.cancel_.store( true, memory_order_release );

                return true;
            }

            return false;
        }


        [[nodiscard]]
        std::tuple< RetCode, NodeUid, NodeLock > lock_path( NodeUid entry_node_uid_,
                        const Key & entry_path_,
                        const Key & relative_path,
                        const execution_chain & incoming,
                        execution_chain & outgoing )
        {
            using namespace std;

            while ( !check_for_doit( incoming, outgoing ) )
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
