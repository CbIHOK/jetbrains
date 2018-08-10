#ifndef __JB__MOUNT_POINT__H__
#define __JB__MOUNT_POINT__H__

#include <memory>

namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::MountPoint
    {
        using Storage = ::jb::Storage< Policies, Pad >;

        using Impl = typename Storage::MountPointImpl;
        std::weak_ptr< Impl > impl_;

        MountPoint(const std::shared_ptr< Impl > impl) noexcept : impl_(impl) {}

    public:

        MountPoint() noexcept = default;

        MountPoint(const MountPoint & o) noexcept = default;

        MountPoint(MountPoint && o) noexcept = default;

        MountPoint & operator = (const MountPoint & o) noexcept = default;

        MountPoint & operator = (MountPoint && o) noexcept = default;

        operator bool() const noexcept { return impl_.lock(); }

        friend bool operator == (const MountPoint & l, const MountPoint & r) noexcept { return l.lock() == r.lock(); }
        friend bool operator != (const MountPoint & l, const MountPoint & r) noexcept { return l.lock() != r.lock(); }

        RetCode Close( bool force = false ) const noexcept
        {
        }
    };
}

#include "mount_point_impl.h"

#endif
