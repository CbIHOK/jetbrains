#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include <memory>
#include <tuple>
#include <exception>


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
        friend class Storage;
        friend class TestVirtualVolume;

        //
        // Few aliases
        //
        using Storage = ::jb::Storage< Policies, Pad >;
        using Value = typename Storage::Value;
        using Key = typename Storage::Key;
        using KeyValue = typename Storage::KeyValue;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPoint = typename Storage::MountPoint;
        using Timestamp = typename Storage::Timestamp;

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
            return l.impl_.lock( ) >= r.impl_.lock( );
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


        std::tuple< RetCode > Insert( const KeyValue & key, const KeyValue & subkey, Value && value, Timestamp && good_before = Timestamp{}, bool overwrite = false ) noexcept
        {
            using namespace std;

            try
            {
                Key key_{ key };
                if ( !key_.is_path( ) )
                {
                    return { RetCode::InvalidKey };
                }

                Key subkey_{ subkey };
                if ( !subkey_.is_leaf( ) )
                {
                    return { RetCode::InvalidSubkey };
                }

                if ( auto impl = impl_.lock( ) )
                {
                    return impl->insert( key_, subkey_, move(value), move(good_before), overwrite );
                }
                else
                {
                    return { RetCode::InvalidHandle };
                }
            }
            catch ( const bad_alloc & )
            {
                return { RetCode::InsufficientMemory };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }


        
        std::tuple< RetCode, Value > Get( const KeyValue & key ) noexcept
        {
            using namespace std;

            try
            {
                Key key_{ key };
                if ( !key_.is_path( ) )
                {
                    return { RetCode::InvalidKey, Value{} };
                }

                if ( auto impl = impl_.lock( ) )
                {
                    return impl->get( key_ );
                }
                else
                {
                    return { RetCode::InvalidHandle, Value{} };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return { RetCode::InsufficientMemory, Value{} };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, Value{} };
        }


        std::tuple< RetCode > Erase( const KeyValue & key, bool force = false ) noexcept
        {
            using namespace std;

            try
            {
                Key key_{ key };
                if ( !key_.is_path( ) )
                {
                    return { RetCode::InvalidKey };
                }

                if ( auto impl = impl_.lock( ) )
                {
                    return impl->erase( key_, force );
                }
                else
                {
                    return { RetCode::InvalidHandle };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return { RetCode::InsufficientMemory };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }


        [[nodiscard]]
        std::tuple< RetCode, MountPoint > Mount( const PhysicalVolume & physical_volume, const KeyValue & physical_path, const KeyValue & logical_path, const KeyValue & alias ) noexcept
        {
            using namespace std;

            try
            {
                Key physical_path_{ physical_path };
                if ( !physical_path_.is_path( ) )
                {
                    return { RetCode::InvalidKey, MountPoint{} };
                }

                Key logical_path_{ logical_path };
                if ( !logical_path_.is_path( ) )
                {
                    return { RetCode::InvalidKey, MountPoint{} };
                }

                Key alias_{ alias };
                if ( !alias_.is_leaf( ) )
                {
                    return { RetCode::InvalidSubkey, MountPoint{} };
                }

                auto impl = impl_.lock( );
                auto physical_impl = physical_volume.impl_.lock( );

                if ( impl && physical_impl )
                {
                    return impl->mount( physical_impl, physical_path_, logical_path_, alias_ );
                }
                else
                {
                    return { RetCode::InvalidHandle, MountPoint{} };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return { RetCode::InsufficientMemory, MountPoint{} };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, MountPoint{} };
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