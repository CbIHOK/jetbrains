#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::MountPointImpl
    {
        friend typename Pad;

        using Key = Storage::Key;
        using KeyValue = typename Key::ValueT;
        using PhysicalVolumeImpl = typename Storage::PhysicalVolumeImpl;
        using NodeLock = typename PhysicalVolumeImpl::NodeLock;
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;

        NodeLock mounted_from_lock_, mounted_to_lock_;
        std::shared_ptr< PhysicalVolumeImpl > physical_volume_;
        KeyValue entry_path_;
        NodeUid entry_uid_;


    public:
        MountPointImpl() {}

        MountPointImpl( MountPointImpl &&) = delete;

        auto physical_volume() const noexcept { return physical_volume_; }
    };
}

#endif
