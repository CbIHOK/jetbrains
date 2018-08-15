#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


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

        using NodeUid = size_t;
        using NodeLock = typename NodeLocker::NodeLock;

        auto get_mount( Key path )
        {
            return std::pair{ RetCode::Ok, std::shared_ptr< MountPointImpl >( ) };
        }
    };
}


#include "node_locker.h"
#include "bloom.h"


#endif
