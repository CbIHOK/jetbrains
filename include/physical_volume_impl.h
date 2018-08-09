#ifndef __JB__PHYSICAL_VOLUME_IMPL__H__
#define __JB__PHYSICAL_VOLUME_IMPL__H__


template < typename Policies, typename Pad > class PhysicalVolume;
template < typename Policies, typename Pad > class MountPointImpl;


namespace jb
{
    template < typename Policies, typename Pad >
    class PhysicalVolume< Policies, Pad >::Impl
    {
        using PhysicalVolume = ::jb::PhysicalVolume< Policies, Pad >;
        using MountPointImpl = ::jb::MountPointImpl< Policies, Pad >;
    };
}


#endif
