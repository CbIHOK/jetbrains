#ifndef __JB__MOUNT_POINT__H__
#define __JB__MOUNT_POINT__H__


#include "physical_volume.h"
#include <atomic>
#include <memory>
#include <tuple>
#include <assert.h>


namespace jb
{
    namespace details
    {
        template < typename Policies, typename TestHooks >
        class virtual_volume;


        template < typename Policies, typename TestHooks = void >
        class mount_point
        {
            template < typename Policies > friend class VirtualVolume;

            using self_type = mount_point< Policies, TestHooks >;
            using MountPointPtr = std::shared_ptr< self_type >;
            using Key = std::basic_string< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
            using KeyView = std::basic_string_view< typename Policies::KeyCharT, typename Policies::KeyCharTraits >;
            using PhysicalVolume = physical_volume< Policies >;
            using PhysicalVolumePtr = std::shared_ptr< PhysicalVolume >;


            RetCode status_ = Ok;
            PhysicalVolumePtr physical_volume_;
            Key physical_path_;
            Key logical_path_;
            MountPointPtr parent_mount_;
            size_t priority_;
            inline static std::atomic< size_t > priority_holder_;


            explicit mount_point( PhysicalVolumePtr physical_volume, Key && physical_path, Key && logical_path ) noexcept
                : physical_volume_( physical_volume )
                , physical_path_( physical_path )
                , logical_path_( logical_path )
                , priority_( priority_holder_.fetch_add( 1 ) )
            {
                assert( physical_volume_ );
                assert( detail::is_valid_path( physical_path_ ) );
                assert( detail::is_valid_path( logical_path_ ) );
                //
                // find and lock physical path
            }

            mount_point( self_type && ) = delete;

            RetCode status() const noexcept { return status_; }
            PhysicalVolumePtr physical_volume() const noexcept { return physical_volume_; }
            KeyView physical_path() const noexcept { return physical_path_; }
            KeyView logical_view() const noexcept { return logical_path_; }
            MountPointPtr parent_mount() const noexcept { return parent_mount_; }
            size_t priority() const noexcept { return priority_; }
        };
    }
}

#endif
