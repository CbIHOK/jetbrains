#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__

namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::MountPoint::Impl
    {
    public:
        Impl() {}

        Impl(Impl &&) = delete;
    };
}

#endif
