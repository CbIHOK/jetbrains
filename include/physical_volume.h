#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


#include <memory>


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
        using RetCode = Storage::RetCode;

    public:

        class Impl;

        friend bool operator == (const PhysicalVolume & l, const PhysicalVolume & r) noexcept
        {
            return true;
        }

        auto get_mount( KeyRefT path )
        {
            return std::pair{ RetCode::Ok, std::shared_ptr< MountPointImpl >() };
        }
    };
}

#endif
