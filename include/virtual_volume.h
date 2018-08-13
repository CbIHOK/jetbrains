#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include <memory>
#include <key.h>


class TestVirtualVolume;


namespace jb
{
    /** Virtual Volume

    Implements monostate pattern, allows many instances to share the same Virtual Volume
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::VirtualVolume
    {
        friend typename Pad;
        template < typename Policies, typename Pad, typename T > friend struct Hash;

        //
        // Few aliases
        //
        using Storage = ::jb::Storage< Policies, Pad >;
        using ValueT = typename Storage::ValueT;
        using KeyT = typename Storage::KeyT;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPoint = typename Storage::MountPoint;
        using TimestampT = typename Storage::TimestampT;


        friend class Storage;
        friend typename PhysicalVolume;
        friend typename MountPoint;


        //
        // PIMP
        //
        using Impl = Storage::VirtualVolumeImpl;
        std::weak_ptr< Impl > impl_;


        /* Instantiating constructor

        Assignes the instance with implementationw

        @param [in] impl - implementation instance to be referred
        @throw nothing
        */
        VirtualVolume( const std::shared_ptr< Impl > & impl ) noexcept : impl_( impl ) {}


    public:

        /** Default constructor

        Creates dummy instance that is not attached to an existing Virtual Volume

        @throw nothing
        */
        VirtualVolume( ) noexcept = default;


        /** Copy constructor

        Initializes new instance as a copy of the origin

        @param [in] o - origin
        @throw nothing
        */
        VirtualVolume( const VirtualVolume & o ) noexcept = default;


        /** Copying assignment

        Sets the instance with a copy of origin

        @param [in] o - origin
        @return lvalue of the instance
        @throw nothing
        */
        VirtualVolume & operator = ( const VirtualVolume & o ) noexcept = default;


        /** Moving constructor

        Initializes new instance by moving a content from given origin

        @param [in] o - origin
        @return lvalue of created instance
        @throw nothing
        */
        VirtualVolume( VirtualVolume && o ) noexcept = default;


        /** Moving assignment

        Sets the instance by moving content from given origin

        @param [in] o - origin
        @return lvalue of the instance
        @throw nothing
        */
        VirtualVolume & operator = ( VirtualVolume && o ) noexcept = default;


        /** Checks if an instance is valid i.e. it represents existing volume

        @return true if instance is valid
        @throw nothing
        */
        operator bool() const noexcept { return (bool)impl_.lock(); }


        /** Comparison operators

        @param [in] l - left part of comparison operator
        @param [in] r - right part of comparison operator
        @return true if the arguments meet condition
        @throw nothing
        */
        friend auto operator == (const VirtualVolume & l, const VirtualVolume & r) noexcept
        {
            return l.impl_.lock( ) == r.impl_.lock( );
        }

        friend auto operator != (const VirtualVolume & l, const VirtualVolume & r) noexcept
        {
            return l.impl_.lock( ) != r.impl_.lock( );
        }

        friend auto operator < ( const VirtualVolume & l, const VirtualVolume & r ) noexcept
        {
            return l.impl_.lock( ) < r.impl_.lock( );
        }

        friend auto operator > ( const VirtualVolume & l, const VirtualVolume & r ) noexcept
        {
            return l.impl_.lock( ) > r.impl_.lock( );
        }

        friend auto operator <= ( const VirtualVolume & l, const VirtualVolume & r ) noexcept
        {
            return l.impl_.lock( ) <= r.impl_.lock( );
        }

        friend auto operator >= ( const VirtualVolume & l, const VirtualVolume & r ) noexcept
        {
            return l.impl_.lock( ) < r.impl_.lock( );
        }

        /** Detaches associated Virtual Volume and close it
        
        If there are concurrent operations in progress on associated Virtual Volume then the volume
        stays alive until all the operation gets completed

        @return std::tuple< ret_code >
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::InvalidHandler => the instance is not attached

        @throw nothing
        */
        auto Close() const noexcept
        {
            return Storage::close( *this );
        }


        auto Insert( KeyT path, KeyT subkey, ValueT && value, TimestampT && timestamp = TimestampT{}, bool overwrite = false ) noexcept
        {
            using namespace std;

            if (auto impl = impl_.lock())
            {
                return impl->Insert( path, subkey, move( value ), move( timestamp ), overwrite );
            }
            else
            {
                return RetCode::InvalidHandle;
            }
        }


        [[ nodiscard ]]
        auto Get( KeyT key ) noexcept
        {
            using namespace std;

            if (auto impl = impl_.lock())
            {
                return impl->Get(key);
            }
            else
            {
                return pair{ RetCode::InvalidHandle, ValueT{} };
            }
        }


        auto Erase( KeyT key, bool force = false ) noexcept
        {
            if (auto impl = impl_.lock())
            {
                return impl->Erase(key, force);
            }
            else
            {
                return RetCode::InvalidHandle;
            }
        }


        [[nodiscard]]
        auto Mount( const PhysicalVolume & physical_volume, KeyT physical_path, KeyT logical_path ) noexcept
        {
            using namespace std;

            if ( !physical_path.is_path( ) || !logical_path.is_path( ) )
            {
                return pair{ RetCode::InvalidKey, MountPoint{} };
            }

            auto impl = impl_.lock( );
            auto physical_impl = physical_volume.impl_.lock( );

            if ( impl && physical_impl )
            {
                return impl->Mount( physical_impl, physical_path, logical_path );
            }
            else
            {
                return pair{ RetCode::InvalidHandle, MountPoint{} };
            }
        }
    };
}

#include "virtual_volume_impl.h"


namespace jb
{
    template < typename Policies, typename Pad >
    struct Hash< Policies, Pad, typename Storage< Policies, Pad >::VirtualVolume >
    {
        static constexpr bool enabled = true;

        size_t operator () ( const typename Storage< Policies, Pad >::VirtualVolume & volume ) const noexcept
        {
            return std::hash< decltype( volume.impl_.lock() ) >{}( volume.impl_.lock() );
        }
    };
}

#endif