#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolume
    {
    public:

        friend bool operator == (const PhysicalVolume & l, const PhysicalVolume & r) noexcept
        {
            return true;
        }
    };
}

#endif
