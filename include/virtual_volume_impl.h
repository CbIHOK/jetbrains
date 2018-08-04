#ifndef __JB__VIRTUAL_VOLUME_IMPL__H__
#define __JB__VIRTUAL_VOLUME_IMPL__H__


#include <boost/container/static_vector.hpp>


namespace jb
{

    template < typename Policies >
    class Storage< Policies >::VirtualVolume::Impl
    {
        class MountPoint;

        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        static constexpr size_t MountPointLimit = Policies::VirtualVolumePolicy::MountPointLimit;

    public:
        Impl() noexcept = default;
        Impl(Impl&&) noexcept = delete;
    };

}


#include "mount_point.h"

#endif
