#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


#include <memory>


namespace jb
{

    template < typename Policies, typename Pad > class Storage;
    template < typename Policies, typename Pad > class VirtualVolume;


    template < typename Policies, typename Pad >
    class PhysicalVolume
    {
        class Impl;

        friend typename Pad;
        template < typename Policies, typename Pad, typename T > friend struct Hash;

        using Storage = ::jb::Storage< Policies, Pad >;
        using VirtualVolume = ::jb::VirtualVolume< Policies, Pad >;

        friend class Storage;
        friend typename VirtualVolume::Impl;


        using KeyCharT = typename Policies::KeyCharT;
        using KeyValueT = typename Policies::KeyValueT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;
        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        using KeyHashF = typename Policies::KeyPolicy::KeyHashF;

        static constexpr size_t MountPointLimit = Policies::VirtualVolumePolicy::MountPointLimit;

        std::weak_ptr< Impl > impl_;

        PhysicalVolume( const std::shared_ptr< Impl > & impl ) noexcept :impl_( impl ) {}


    public:



        auto get_mount( KeyRefT path )
        {
            return std::pair{ RetCode::Ok, std::shared_ptr< MountPointImpl >() };
        }

        PhysicalVolume() noexcept = default;

        PhysicalVolume( const PhysicalVolume & o ) noexcept = default;

        PhysicalVolume( PhysicalVolume && o ) noexcept = default;

        PhysicalVolume & operator = ( const PhysicalVolume & o ) noexcept = default;

        PhysicalVolume & operator = ( PhysicalVolume && o ) noexcept = default;

        operator bool() const noexcept { return ( bool )impl_.lock(); }

        friend bool operator == ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() == r.impl_.lock();
        }

        friend bool operator != ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() != r.impl_.lock();
        }

        friend bool operator < ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() < r.impl_.lock();
        }

        friend bool operator > ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() > r.impl_.lock();
        }

        friend bool operator <= ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() <= r.impl_.lock();
        }

        friend bool operator >= ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock() >= r.impl_.lock();
        }

        RetCode Close() const noexcept
        {
            return Storage::close( *this );
        }

        RetCode PrioritizeOnTop() const noexcept
        {
            return Storage::prioritize_on_top( *this );
        }

        RetCode PrioritizeOnBottom() const noexcept
        {
            return Storage::prioritize_on_bottom( *this );
        }

        RetCode PrioritizeBefore( const PhysicalVolume & before ) const noexcept
        {
            return Storage::prioritize_on_top( *this, before );
        }

        RetCode PrioritizeAfter( const PhysicalVolume & after ) const noexcept
        {
            return Storage::prioritize_on_top( *this, after );
        }
    };
}


#include "physical_volume_impl.h"


namespace jb
{
    template < typename Policies, typename Pad >
    struct Hash < typename Policies, typename Pad, PhysicalVolume< Policies, Pad > >
    {
        static constexpr bool enabled = true;

        size_t operator () ( const PhysicalVolume< Policies, Pad > & volume ) const noexcept
        {
            void * p = reinterpret_cast< void* >( volume.impl_.lock().get() );
            return std::hash< void* >()( p );
        }
    };
}

#endif
