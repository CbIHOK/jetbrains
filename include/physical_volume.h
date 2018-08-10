#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


#include <memory>

namespace jb
{

    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolume
    {
        friend typename Pad;
        friend class TestStorage;

        template < typename T, typename Policies, typename Pad > friend struct Hash;

        using Storage = ::jb::Storage< Policies, Pad >;

        friend class Storage;
        friend typename Storage::VirtualVolume;

        using KeyCharT = typename Policies::KeyCharT;
        using KeyValueT = typename Policies::KeyValueT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;
        using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
        using KeyHashF = typename Policies::KeyPolicy::KeyHashF;

        using Impl = typename Storage::PhysicalVolumeImpl;
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
}


#include "physical_volume_impl.h"


namespace jb
{
    template < typename Policies, typename Pad >
    struct Hash< typename Storage< Policies, Pad >::PhysicalVolume, typename Policies, typename Pad >
    {
        static constexpr bool enabled = true;

        size_t operator () ( const typename Storage< Policies, Pad >::PhysicalVolume & volume ) const noexcept
        {
            return std::hash< decltype( volume.impl_.lock( ) ) >{}( volume.impl_.lock( ) );
        }
    };
}

#endif
