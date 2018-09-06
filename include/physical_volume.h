#ifndef __JB__PHYSICAL_VOLUME__H__
#define __JB__PHYSICAL_VOLUME__H__


#include <memory>


namespace jb
{
    /** Represents handle of physical volume to be used at the user-end

    @tparam Policies - global settings
    */
    template < typename Policies >
    class Storage< Policies >::PhysicalVolume
    {
        using Storage = ::jb::Storage< Policies >;
        using RetCode = typename Storage::RetCode;


        //
        // friends
        //
        friend class TestStorage;
        friend class Storage;
        friend typename Storage::VirtualVolume;
        template < typename T > friend struct Storage::Hash;


        //
        // data members
        //
        using Impl = typename Storage::PhysicalVolumeImpl;
        std::weak_ptr< Impl > impl_;


        /* Explicit constructor, creates a handle basing on given PIMP

        @param [in] impl - PIMP
        @throw nothing
        */
        explicit PhysicalVolume( const std::shared_ptr< Impl > & impl ) noexcept :impl_( impl ) {}


    public:

        /** Default constructor, creates dummy physical volume handle
        */
        PhysicalVolume() noexcept = default;


        /** The class provides both copy/move construction
        */
        PhysicalVolume( const PhysicalVolume & ) noexcept = default;
        PhysicalVolume( PhysicalVolume && ) noexcept = default;


        /** The class provides both copy/move assignment
        */
        PhysicalVolume & operator = ( const PhysicalVolume & ) noexcept = default;
        PhysicalVolume & operator = ( PhysicalVolume && ) noexcept = default;


        /** operator bool()

        @retval bool - true if refers valid physical volume object
        @throws nothing
        */
        operator bool() const noexcept { return ( bool )impl_.lock(); }


        /** Compare operators

        @retval bool - true if operands meet the condition
        @throws nothing
        */
        friend bool operator == ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept { return l.impl_.lock() == r.impl_.lock(); }
        friend bool operator != ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept { return l.impl_.lock() != r.impl_.lock(); }
        friend bool operator < ( const PhysicalVolume & l, const PhysicalVolume & r ) noexcept { return l.impl_.lock( ) < r.impl_.lock( ); }


        /** Closes associated physical volume and release allocated resources

        @retval RetCode - operation status
        @throw nothing
        */
        RetCode Close() const noexcept
        {
            return Storage::close( *this );
        }


        /** Sets associated physical volume as the one with the highest priority

        @retval RetCode - operation status
        @throw nothing
        */
        RetCode PrioritizeOnTop() const noexcept
        {
            try
            {
                Storage::prioritize_on_top( *this );
                return RetCode::Ok;
            }
            catch ( ... )
            {
            }
            return RetCode::UnknownError;
        }


        /** Sets associated physical volume as the one with the lowest priority

        @retval RetCode - operation status
        @throw nothing
        */
        RetCode PrioritizeOnBottom() const noexcept
        {
            try
            {
                Storage::prioritize_on_bottom( *this );
                return RetCode::Ok;
            }
            catch ( ... )
            {
            }
            return RetCode::UnknownError;
        }


        /** Sets associated physical volume as the one having priority greater than given one

        @param [in] before - the volume to be prioritized below
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode PrioritizeBefore( const PhysicalVolume & before ) const noexcept
        {
            try
            {
                Storage::prioritize_before( *this, before );
                return RetCode::Ok;
            }
            catch ( ... )
            {
            }
            return RetCode::UnknownError;
        }


        /** Sets associated physical volume as the one having priority lower than given one

        @param [in] before - the volume to be prioritized above
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode PrioritizeAfter( const PhysicalVolume & after ) const noexcept
        {
            try
            {
                Storage::prioritize_after( *this, after );
                return RetCode::Ok;
            }
            catch ( ... )
            {
            }
            return RetCode::UnknownError;
        }
    };
}


#include "physical_volume_impl.h"


#endif
