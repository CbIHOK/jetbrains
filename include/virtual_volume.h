#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include <memory>
#include <tuple>
#include <exception>


class TestVirtualVolume;


namespace jb
{

    /** Virtual Volume

    Implements user-end handle of virtual volume

    @tparam Policies - global setting
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


        /* Explicit private constructor, creates an handle with given PIMP instance

        @param [in] impl - implementation instance to be referred
        @throw nothing
        */
        explicit VirtualVolume( const std::shared_ptr< Impl > & impl ) noexcept : impl_( impl ) {}


    public:

        /** Default constructor, creates dummy virtual volume handle

        @throw nothing
        */
        VirtualVolume( ) noexcept = default;


        /** The class is copy/move constructible
        */
        VirtualVolume( const VirtualVolume & ) noexcept = default;
        VirtualVolume( VirtualVolume && ) noexcept = default;


        /** The class provides copy/move assignment
        */
        VirtualVolume & operator = ( const VirtualVolume & ) noexcept = default;
        VirtualVolume & operator = ( VirtualVolume && ) noexcept = default;


        /** operator bool()

        @retval bool - true if the handle is associated with virtual volume
        @throw nothing
        */
        operator bool() const noexcept { return (bool)impl_.lock(); }


        /** Comparison operators

        @return bool true if the arguments meet condition
        @throw nothing
        */
        friend auto operator == (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.impl_.lock( ) == r.impl_.lock( ); }
        friend auto operator != (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.impl_.lock( ) != r.impl_.lock( ); }
        friend auto operator < ( const VirtualVolume & l, const VirtualVolume & r ) noexcept { return l.impl_.lock( ) < r.impl_.lock( ); }


        /** Detaches associated Virtual Volume and close it
        
        If there are concurrent operations in progress on associated Virtual Volume then the volume
        stays alive until all the operation gets completed

        @return std::tuple< ret_code >
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::InvalidHandler => the instance is not attached

        @throw nothing
        */
        RetCode Close() noexcept
        {
            return Storage::close( *this );
        }


        /** Inserts subkey with specified value and expiration timemark at given path

        @param [in] key - insertion path
        @param [in] subkey - subkey to be inserted
        @param [in] value - subkey's value
        @param [in] good_before - subkey's expiration timemark (in msecs from epoch)
        @param [in] overwrite - allows to overwrite existing subkey
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode Insert( const KeyValue & key, const KeyValue & subkey, const Value & value, uint64_t good_before = 0, bool overwrite = false ) noexcept
        {
            using namespace std;

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
                return impl->insert( key_, subkey_, value, good_before, overwrite );
            }
            else
            {
                return { RetCode::InvalidHandle };
            }
        }


        /** Provides value of specified key

        @param [in] key - key to be read
        @retval RetCode - operation status
        @retval Value - key's value
        @throw nothing
        */
        std::tuple< RetCode, Value > Get( const KeyValue & key ) noexcept
        {
            using namespace std;

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


        /** Erases given key

        @param [in] key - key to be removed
        @param [in] force - allows to erase key's children subtree if exist
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode Erase( const KeyValue & key, bool force = false ) noexcept
        {
            using namespace std;

            Key key_{ key };

            if ( !key_.is_path( ) )
            {
                return RetCode::InvalidKey;
            }
            else if ( force )
            {
                return RetCode::NotYetImplemented;
            }
            else if ( auto impl = impl_.lock( ) )
            {
                return impl->erase( key_, force );
            }
            else
            {
                return RetCode::InvalidHandle;
            }
        }


        /** Mounts specified path of physical volume at given logical path with given alias

        @param [in] physical_volume - physical volume
        @param [in] physical_path - physical path to be mounted
        @param [in] logical_path - path to new mount point
        @param [in] alias - name of new mount point
        @retval RetCode - operation status
        @retval MountPoint - mount point handle
        @throw nothing
        */
        std::tuple< RetCode, MountPoint > Mount( const PhysicalVolume & physical_volume, const KeyValue & physical_path, const KeyValue & logical_path, const KeyValue & alias ) noexcept
        {
            using namespace std;

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
    };
}


#include "virtual_volume_impl.h"


#endif