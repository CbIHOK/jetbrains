#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__


namespace jb
{
    template < typename Policies, typename Pad >
    class MountPointImpl
    {
        friend typename Pad;

    public:
        MountPointImpl() {}

        MountPointImpl( MountPointImpl &&) = delete;
    };
}

#endif
