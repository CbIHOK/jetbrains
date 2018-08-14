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

        using NodeUid = size_t;

    public:

        auto get_mount( Key path )
        {
            return std::pair{ RetCode::Ok, std::shared_ptr< MountPointImpl >( ) };
        }

        class NodeLocker;
    };
}


#include "node_locker.h"


#endif
