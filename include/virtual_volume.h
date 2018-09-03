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
    template < typename Policies >
    class Storage< Policies >::VirtualVolume
    {
        friend class TestVirtualVolume;

        using Storage = ::jb::Storage< Policies >;
        friend class Storage;

        //
        // Few aliases
        //
        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using KeyValue = typename Storage::KeyValue;
        using Value = typename Storage::Value;
        using PhysicalVolume = typename Storage::PhysicalVolume;
        using MountPoint = typename Storage::MountPoint;

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


        std::tuple< RetCode > Insert( const KeyValue & key, const KeyValue & subkey, Value && value, uint64_t good_before = 0, bool overwrite = false ) noexcept
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

                uint64_t now = chrono::system_clock::now().time_since_epoch() / chrono::milliseconds( 1 );

                if ( good_before && good_before < now )
                {
                    return { RetCode::AlreadyExpired };
                }

                if ( auto impl = impl_.lock( ) )
                {
                    return impl->insert( key_, subkey_, std::move( value ), good_before, overwrite );
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
                    if ( auto[ ret, mp_impl ] = impl->mount( physical_impl, physical_path_, logical_path_, alias_ );  RetCode::Ok == ret )
                    {
                        return { RetCode::Ok, MountPoint{ mp_impl, impl } };
                    }
                    else
                    {
                        return { ret, MountPoint{} };
                    }
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


#endif