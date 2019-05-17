#ifndef __JB__STORAGE__H__
#define __JB__STORAGE__H__

#include "ret_codes.h"
#include "physical_volume.h"
#include "virtual_volume.h"
#include <mutex>
#include <unordered_set>
#include <tuple>
#include <filesystem>
#include <memory>


namespace jb
{
    /**
    */
    template < typename Policies >
    class Storage
    {
        template < typename VolumeT >
        static auto singletons()
        {
            static std::mutex guard;
            static std::unordered_set< std::shared_ptr< typename VolumeT::Impl > > holder;
            return std::forward_as_tuple( guard, holder );
        }


        template < typename VolumeT, typename ...Args >
        static std::tuple< RetCode, std::weak_ptr< VolumeT > > open( Args&&... args ) noexcept
        {
            try
            {
                auto[ guard, holder ] = singletons< VolumeT >();

                auto impl = std::make_shared< VolumeT >( std::forward( args )... );
                assert( impl );

                if ( Ok == impl->status() )
                {
                    std::unique_lock lock( guard );

                    if ( auto ok = holder.insert( impl ).second )
                    {
                        return { Ok, std::weak_ptr< VolumeT >{ impl } };
                    }
                    else
                    {
                        return { UnknownError, std::weak_ptr< VolumeT >{} };
                    }
                }
                else
                {
                    return { impl->status(), std::weak_ptr< VolumeT >{} };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return { InsufficientMemory, std::weak_ptr< VolumeT >{} };
            }
            catch ( ... )
            {
                return { UnknownError, std::weak_ptr< VolumeT >{} };
            }
        }


    public:

        using VirtualVolume = ::jb::VirtualVolume< Policies >;
        using PhysicalVolume = ::jb::PhysicalVolume < Policies >;
        using MountPoint = typename VirtualVolume::MountPoint;
        using Key = typename VirtualVolume::Key;
        using Value = typename VirtualVolume::Value;


        static std::tuple < RetCode, std::weak_ptr< VirtualVolume > > open_VirtualVolume() noexcept
        {
            return open< VirtualVolume >();
        }


        static std::tuple < RetCode, std::weak_ptr< PhysicalVolume > > open_PhysicalVolume( const std::filesystem::path & path, size_t priority = 0 ) noexcept
        {
            return open< PhysicalVolume >( path, priority );
        }


        template< typename VolumeT >
        static RetCode close( std::weak_ptr< VolumeT > volume ) noexcept
        {
            try
            {
                auto[ guard, holder ] = singletons< VolumeT >();

                if ( auto impl = volume.lock() )
                {
                    std::unique_lock lock( guard );

                    if ( auto it = holder.find( impl ); holder.end != it )
                    {
                        holder.erase( it );
                        volume.reset();
                        
                        return Ok;
                    }
                }

                return InvalidHandle;
            }
            catch ( ... )
            {
                return UnknownError;
            }
        }


        /** Closes all oped volumes

        @retval RetCode - operation status
        @throw nothing
        */
        static RetCode close_all( ) noexcept
        {
            try
            {
                {
                    auto[ guard, holder ] = singletons< VirtualVolume >();
                    std::unique_lock lock( guard );
                    holder.clear();
                }
                {
                    auto[ guard, holder ] = singletons< PhysicalVolume >();
                    std::unique_lock lock( guard );
                    holder.clear();
                }

                return Ok;
            }
            catch ( ... )
            {
                return UnknownError;
            }
        }
    };
}

#endif
