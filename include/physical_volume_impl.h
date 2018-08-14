#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


class TestNodeLocker;

namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl
    {
        friend class TestNodeLocker;

        using Storage = ::jb::Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPointImpl = typename Storage::MountPointImpl;

        class NodeLocker;

    public:

        using NodeUid = size_t;
        using NodeLock = typename NodeLocker::NodeLock;

        auto get_mount( Key path )
        {
            return std::pair{ RetCode::Ok, std::shared_ptr< MountPointImpl >( ) };
        }
    };
}


#include "node_locker.h"


#endif
