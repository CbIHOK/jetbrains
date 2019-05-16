#ifndef __JB__VirtualVolume__H__
#define __JB__VirtualVolume__H__


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


        static bool is_valid_key( KeyView key )
        {
            using Regex = std::basic_regex< KeyView::value_type >;
            using RegexStr = typename Regex::string_type;

            static const auto pattern = R"noesc(^(\/[a-zA-Z][\w-]*)+$|^\/$)noesc"s;
            static const Regex re{ RegexStr{ pattern.begin(), pattern.end() } };
            return std::regex_match( key.begin(), key.end(), re );
        }

        static bool is_valid_key_segment( KeyView key )
        {
            using Regex = std::basic_regex< KeyView::value_type >;
            using RegexStr = typename Regex::string_type;

            static const auto pattern = R"noesc(^([a-zA-Z][\w-]*)$)noesc"s;
            static const Regex re{ RegexStr{ pattern.begin(), pattern.end() } };
            return std::regex_match( key.begin(), key.end(), re );
        }


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
            if ( auto impl = impl_.lock() )
            {
                try
                {
                    if ( !is_valid_key( key ) )
                    {
                        return RetCode::InvalidKey;
                    }

                    if ( !is_valid_key_segment( subkey ) )
                    {
                        return RetCode::InvalidKey;
                    }
                }
                catch ( ... )
                {
                    return RetCode::UnknownError;
                }

                uint64_t now = chrono::system_clock::now().time_since_epoch() / 1ms;
                if ( good_before && good_before < now )
                {
                    return RetCode::AlreadyExpired;
                }

                return impl->insert( key, subkey, value, good_before, overwrite );
            }
            else
            {
                return RetCode::InvalidVirtualVolumeHandle;
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
            if ( auto impl = impl_.lock() )
            {
                try
                {
                    if ( !is_valid_key( key ) )
                    {
                        return { RetCode::InvalidKey, Value{} };
                    }
                }
                catch ( ... )
                {
                    return { RetCode::UnknownError, Value{} };
                }

                return impl->get( key );
            }
            else
            {
                return { RetCode::InvalidVirtualVolumeHandle, Value{} };
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
            if ( auto impl = impl_.lock() )
            {
                try
                {
                    if ( ! is_valid_key( key ) )
                    {
                        return RetCode::InvalidKey;
                    }
                }
                catch ( ... )
                {
                    return RetCode::UnknownError;
                }

                return impl->erase( key, force );
            }
            else
            {
                return RetCode::InvalidVirtualVolumeHandle;
            }
        }


        /** Mounts specified path of physical volume at given logical path with given alias

        @param [in] PhysicalVolume - physical volume
        @param [in] physical_path - physical path to be mounted
        @param [in] logical_path - path to new mount point
        @param [in] alias - name of new mount point
        @retval RetCode - operation status
        @retval MountPoint - mount point handle
        @throw nothing
        */
        std::tuple< RetCode, MountPoint > Mount( const PhysicalVolume & PhysicalVolume, const KeyView & physical_path, const KeyView & logical_path, const KeyView & alias ) noexcept
        {
            if ( auto VirtualVolume_impl = impl_.lock() )
            {
                if ( auto PhysicalVolume_impl = PhysicalVolume.impl_.lock() )
                {
                    try
                    {
                        if ( !is_valid_key( physical_path ) )
                        {
                            return { RetCode::InvalidPhysicalPath, MountPoint{} };
                        }

                        if ( !is_valid_key( logical_path ) )
                        {
                            return { RetCode::InvalidLogicalPath, MountPoint{} };
                        }

                        if ( !is_valid_key_segment( alias ) )
                        {
                            return { RetCode::InvalidMountAlias, MountPoint{} };
                        }
                    }
                    catch ( ... )
                    {
                        return { RetCode::UnknownError, MountPoint{} };
                    }

                    if ( auto[ ret, mp_impl ] = VirtualVolume_impl->mount( PhysicalVolume_impl, physical_path, logical_path, alias ); RetCode::Ok == ret && mp_impl )
                    {
                        return { RetCode::Ok, MountPoint{ mp_impl, VirtualVolume_impl } };
                    }
                    else
                    {
                        return { ret, MountPoint{} };
                    }
                }
                else
                {
                    return { RetCode::InvalidPhysicalVolumeHandle, MountPoint{} };
                }
            }
            else
            {
                return { RetCode::InvalidVirtualVolumeHandle, MountPoint{} };
            }
        }
    };
}


#include "VirtualVolume_impl.h"


#endif