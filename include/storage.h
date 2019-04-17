#ifndef __JB__STORAGE__H__
#define __JB__STORAGE__H__


#include <mutex>
#include <unordered_set>
#include <tuple>
#include <memory>
#include <filesystem>


namespace jb
{

    /**
    */
    template < typename Policies >
    class Storage
    {

        //
        // export public aliases
        //
        using
        using KeyValue = typename Key::ValueT;
        using Value = typename Policies::ValueT;


        //
        // Provides hash combining constant depending on size of size_t type
        //
        //static constexpr size_t hash_constant() noexcept
        //{
        //    static_assert( sizeof( size_t ) == sizeof( uint32_t ) || sizeof( size_t ) == sizeof( uint64_t ), "Cannot detect 32-bit or 64-bit platform" );

        //    if constexpr ( sizeof( size_t ) == 8 )
        //    {
        //        return 0x9E3779B97F4A7C15ULL;
        //    }
        //    else
        //    {
        //        return 0x9e3779b9U;
        //    }
        //}


        //
        // Provides comined hash value for a variadic sequence of agruments
        //
        //template < typename T, typename... Args >
        //static auto variadic_hash( const T & v, const Args &... args ) noexcept
        //{
        //    auto seed = variadic_hash( args... );
        //    return Hash< T >{}( v ) + hash_constant() + ( seed << 6 ) + ( seed >> 2 );
        //}


        ////
        //// Just a terminal specialization of variadic template
        ////
        //template < typename T >
        //static auto variadic_hash( const T & v ) noexcept
        //{
        //    return Hash< T >{}( v );
        //}


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

                auto impl = std::make_shared< VolumeT >( std::move( args )... );
                assert( impl );

                if ( RetCode::Ok != impl->status() )
                {
                    return { pimp->status(), std::weak_ptr< VolumeT >{} };
                }

                std::unique_lock lock( guard );

                if ( auto ok = holder.insert( pimp ).second; !ok )
                {
                    return { RetCode::UnknownError, std::weak_ptr< VolumeT >{} };
                }

                return { RetCode::Ok, std::weak_ptr< VolumeT::Impl >{ pimp } };
            }
            catch ( const std::bad_alloc & )
            {
                return { RetCode::InsufficientMemory, VolumeT{} };
            }
            catch ( ... )
            {
                return { RetCode::UnknownError, VolumeT{} };
            }
        }


    public:

        /** Enumerates all possible return codes
        */
        enum class RetCode
        {
            Ok,                     ///< Operation succedded
            InvalidHandle,          ///< Given handle does not address valid object
            InvalidVirtualVolume,
            InvalidPhysicalVolume,
            InvalidMountPoint,
            VolumeAlreadyMounted,   ///< Attempt to mount the same physical volume at the same logical path
            InvalidKey,             ///< Invalid key value
            InvalidMountAlias,      ///< Invalid subkey value
            InvalidLogicalPath,     ///< Given logical path cannot be mapped onto a physical one
            InvalidPhysicalPath,
            PathLocked,             ///< Given path is locked due to mounting 
            NotFound,               ///< Such path does not have a physical representation
            InUse,                  ///< The handler is currently used by concurrent operation and cannot be closed
            HasDependentMounts,     ///< There are underlaying mount
            MaxTreeDepthExceeded,   ///< Cannot search so deep inside
            SubkeyLimitReached,     ///< Too many subkeys
            AlreadyExpired,         ///< Given timestamp already in the past
            AlreadyExists,          ///< Key already exists
            NotLeaf,                ///< Erased node not a leaf
            IncompatibleFile,       ///< File is incompatible
            AlreadyOpened,          ///< Physical file is already opened
            UnableToOpen,           ///< Cannot open specified file
            TooManyConcurrentOps,   ///< The limit of concurent operations over physical volume is reached
            IoError,                ///< General I/O error
            InvalidData,            ///< Data read from storage file is invalid
            InsufficientMemory,     ///< Operation failed due to low memory
            UnknownError,           ///< Something wrong happened
            NotYetImplemented
        };


        //
        // public classes
        //
        class VirtualVolume;
        class PhysicalVolume;
        class MountPoint;


        static std::tuple < RetCode, std::weak_ptr< VirtualVolume > > open_virtual_volume() noexcept
        {
            return open< VirtualVolume >();
        }

        static std::tuple < RetCode, std::weak_ptr< PhysicalVolume > > open_physical_volume( const std::filesystem::path & path, size_t priority = 0 ) noexcept
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
                        
                        return RetCode::Ok;
                    }
                    else
                    {
                        return RetCode::InvalidHandle;
                    }
                }
                else
                {
                    return RetCode::InvalidHandle;
                }
            }
            catch ( ... )
            {
                return RetCode::UnknownError;
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

                return RetCode::Ok;
            }
            catch(...)
            {
                return RetCode::UnknownError;
            }
        }
    };
}


#include "virtual_volume.h"
#include "physical_volume.h"
#include "mount_point.h"


#endif
