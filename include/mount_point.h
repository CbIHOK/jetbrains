#ifndef __JB__MOUNT_POINT__H__
#define __JB__MOUNT_POINT__H__

#include <memory>
#include <tuple>

namespace jb
{
    /** Replesent Mount Point handle object

    @tparam Policies - global setting
    @tparam Pad - test stuff
    */
    template < typename Policies >
    class Storage< Policies >::MountPoint
    {
        using VirtualVolume = typename Storage::VirtualVolume;
        using VirtualVolumeImpl = typename Storage::VirtualVolumeImpl;

        friend typename Storage::VirtualVolume;
        friend typename Storage::VirtualVolumeImpl;

        using Impl = typename Storage::MountPointImpl;
        std::weak_ptr< Impl > impl_;
        std::weak_ptr< VirtualVolumeImpl > volume_impl_;


        /*
        */
        MountPoint( const std::shared_ptr< Impl > & impl, const std::shared_ptr< VirtualVolumeImpl > & volume_impl ) noexcept
            : impl_( impl )
            , volume_impl_( volume_impl)
        {
        }


    public:

        /** Default constructor, creates dummy handle

        @throw nothing
        */
        MountPoint() noexcept = default;


        /** Copy/move constructors

        @param [in] o - original object
        @throw nothing
        */
        MountPoint(const MountPoint & o) noexcept = default;
        MountPoint(MountPoint && o) noexcept = default;


        /** Copy/move operators

        @param [in] o - original object
        @return assigned instance by lval
        @throw nothing
        */
        MountPoint & operator = (const MountPoint & o) noexcept = default;
        MountPoint & operator = (MountPoint && o) noexcept = default;


        /** Checks if the instance represent valid mount

        @return true if the instance is valid handle
        @throw nothing
        */
        operator bool() const noexcept { return (bool)impl_.lock(); }


        /** Compare operators

        @return true if the instances meet condition
        @throw nothing
        */
        friend bool operator == (const MountPoint & l, const MountPoint & r) noexcept { return l.impl_.lock() == r.impl_.lock(); }
        friend bool operator != (const MountPoint & l, const MountPoint & r) noexcept { return l.impl_.lock() != r.impl_.lock(); }


        /** Unmount the mount point

        @param [in] force - unmount underlaying mounts
        @retval RetCode - operation status
        @throw nothing
        */
        RetCode Close( bool force = false ) const noexcept
        {
            if ( auto volume = volume_impl_.lock( ) )
            {
                return volume->unmount( *this, force );
            }
            else
            {
                return { RetCode::UnknownError };
            }
        }
    };
}

#include "mount_point_impl.h"

#endif
