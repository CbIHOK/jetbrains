#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


#include <memory>

namespace jb
{

    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolume
    {
        using Storage = ::jb::Storage< Policies, Pad >;

        friend typename Pad;
        friend class TestStorage;
        friend class Storage;
        friend typename Storage::VirtualVolume;
        template < typename Policies, typename Pad, typename T > friend struct Hash;

        using Impl = typename Storage::PhysicalVolumeImpl;
        std::weak_ptr< Impl > impl_;

        PhysicalVolume( const std::shared_ptr< Impl > & impl ) noexcept :impl_( impl ) {}


    public:

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
            return l.impl_.lock( ) < r.impl_.lock( );
        }

        friend bool operator > ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock( ) > r.impl_.lock( );
        }

        friend bool operator <= ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock( ) <= r.impl_.lock( );
        }

        friend bool operator >= ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept
        {
            return l.impl_.lock( ) > r.impl_.lock( );
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
            return Storage::prioritize_before( *this, before );
        }

        RetCode PrioritizeAfter( const PhysicalVolume & after ) const noexcept
        {
            return Storage::prioritize_after( *this, after );
        }
    };


    template < typename Policies, typename Pad >
    struct Hash< Policies, Pad, typename Storage< Policies, Pad >::PhysicalVolume >
    {
        static constexpr bool enabled = true;

        size_t operator () ( const typename Storage< Policies, Pad >::PhysicalVolume & volume ) const noexcept
        {
            return std::hash< decltype( volume.impl_.lock( ) ) >{}( volume.impl_.lock( ) );
        }
    };
}


#include "physical_volume_impl.h"


#endif
