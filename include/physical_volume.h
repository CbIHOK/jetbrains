#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolume
    {
        using KeyCharT = typename Policies::KeyCharT;
        using KeyValueT = typename Policies::KeyValueT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;
        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        using KeyHashF = typename Policies::KeyPolicy::KeyHashF;
        static constexpr size_t MountPointLimit = Policies::VirtualVolumePolicy::MountPointLimit;
        
        using MountPoint = typename Storage::MountPoint;
        using MountPointImpl = typename Storage::MountPoint::Impl;
        using MountPointImplP = std::shared_ptr< MountPointImpl >;

    public:

        friend bool operator == (const PhysicalVolume & l, const PhysicalVolume & r) noexcept
        {
            return true;
        }

        std::shared_ptr< MountPointImpl > get_mount( KeyRefT path )
        {
            return std::shared_ptr< MountPointImpl >();
        }
    };
}

#endif
