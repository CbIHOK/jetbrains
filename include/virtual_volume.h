#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include <memory>


namespace jb
{
    /** Virtual Volume Handler

    Implements monostate pattern, allows many instances to share the same Virtual Tome
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::VirtualVolume
    {

        friend class Storage;
        friend typename Pad;


        //
        // Few aliases
        //
        using ValueT = typename Policies::ValueT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;


        //
        // PIMP
        //
        class Impl;
        std::weak_ptr< Impl > impl_;


        /* Instantiating constructor

        Assignes the instance with implementationw

        @param [in] impl - implementation instance to be referred
        @throw nothing
        */
        VirtualVolume(const std::shared_ptr< Impl > & impl) noexcept : impl_(impl) {}


    public:

        /** Default constructor

        Creates dummy instance that is not attached to an existing Virtual Volume

        @throw nothing
        */
        VirtualVolume() noexcept = default;


        /** Copy constructor

        Initializes new instance as a copy of the origin

        @param [in] o - origin
        @throw nothing
        */
        VirtualVolume(const VirtualVolume & o) noexcept = default;


        /** Copying assignment

        Sets the instance with a copy of origin

        @param [in] o - origin
        @return lvalue of the instance
        @throw nothing
        */
        VirtualVolume & operator = (const VirtualVolume & o) noexcept = default;


        /** Moving constructor

        Initializes new instance by moving a content from given origin

        @param [in] o - origin
        @return lvalue of created instance
        @throw nothing
        */
        VirtualVolume(VirtualVolume && o) noexcept = default;


        /** Moving assignment

        Sets the instance by moving content from given origin

        @param [in] o - origin
        @return lvalue of the instance
        @throw nothing
        */
        VirtualVolume & operator = (VirtualVolume && o) noexcept = default;


        /** Checks if an instance is valid i.e. it represents existing volume

        @return true if instance is valid
        @throw nothing
        */
        operator bool() const noexcept { return impl_.lock(); }


        /** Comparison operators

        @param [in] l - left part of comparison operator
        @param [in] r - right part of comparison operator
        @return true if the arguments meet condition
        @throw nothing
        */
        friend auto operator == (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.load() == r.load(); }
        friend auto operator != (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.load() != r.load(); }


        /** Detaches associated Virtual Volume and close it
        
        If there are concurrent operations in progress on associated Virtual Volume then the volume
        stays alive until all the operation gets completed

        @return std::tuple< ret_code >
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::InvalidHandler => the instance is not attached

        @throw nothing
        */
        auto Close() noexcept
        {
            return Storage::close( std::move(*this) );
        }


        auto Insert(KeyRefT path, KeyRefT subkey, ValueT && value, bool overwrite = false) noexcept
        {
            if (auto impl = impl_.lock())
            {
                return impl->Insert(path, subkey, value, overwrite);
            }
            else
            {
                return std::tuple(Storage::RetCode::InvalidHandle);
            }
        }

        auto Get(KeyRefT key) noexcept
        {
            if (auto impl = impl_.lock())
            {
                return impl->Get(key);
            }
            else
            {
                return std::tuple(Storage::RetCode::InvalidHandle);
            }
        }

        auto Erase(KeyRefT key, bool force = false) noexcept
        {
            if (auto impl = impl_.lock())
            {
                return impl->Erase(key, force);
            }
            else
            {
                return std::tuple(Storage::RetCode::InvalidHandle);
            }
        }

        auto Mount(PhysicalVolume physical_volume, KeyRefT physical_path, KeyRefT logical_path) noexcept
        {
            if (auto impl = impl_.lock(); impl)
            {
                return impl->Mount(physical_volume, physical_path, logical_path);
            }
            else
            {
                return std::pair( RetCode::InvalidHandle, MountPoint() );
            }
        }
    };
}

#include "virtual_volume_impl.h"

#endif