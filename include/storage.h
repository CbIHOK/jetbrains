#ifndef __JB__STORAGE__H__
#define __JB__STORAGE__H__


#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <type_traits>
#include <filesystem>
#include <limits>
#include <execution>
#include <assert.h>


#ifndef _NOEXCEPT_
#define _NOEXCEPT_ noexcept
#endif


class TestStorage;
class TestKey;
class TestNodeLocker;
class TestBloom;
class TestPackedValue;
template < typename T > class TestStorageFile;
template < typename T > class TestBTree;

namespace jb
{

    /**
    */
    template < typename Policies >
    class Storage
    {

    public:

        /** Defines hash operator
        */
        template < typename T >
        struct Hash
        {
            size_t operator() ( const T & v ) const noexcept { return std::hash< T >{}( v ); }
        };


    private:

        //
        // let apply Hash to internal types
        //
        template < typename T > friend struct Hash;


        //
        // tests need access to internal classes
        //
        friend class TestStorage;
        friend class TestKey;
        friend class TestNodeLocker;
        friend class TestBloom;
        friend class TestPackedValue;
        template < typename T > friend class TestStorageFile;
        template < typename T > friend class TestBTree;


        //
        // needs access to private methods
        //
        friend class VirtualVolume;
        friend class PhysicalVolume;
        friend class MountPoint;


        //
        // interanal classes
        //
        class Key;
        class VirtualVolumeImpl;
        class PhysicalVolumeImpl;
        class MountPointImpl;


    public:

        /** Enumerates all possible return codes
        */
        enum class RetCode
        {
            Ok,                     ///< Operation succedded
            InvalidHandle,          ///< Given handle does not address valid object
            LimitReached,           ///< All handles of a type are alreay exhausted
            VolumeAlreadyMounted,   ///< Attempt to mount the same physical volume at the same logical path
            InvalidKey,             ///< Invalid key value
            InvalidSubkey,          ///< Invalid subkey value
            InvalidLogicalPath,     ///< Given logical path cannot be mapped onto a physical one
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
        // export public aliases
        //
        using KeyValue = typename Key::ValueT;
        using Value = typename Policies::ValueT;


        //
        // public classes
        //
        class VirtualVolume;
        class PhysicalVolume;
        class MountPoint;


        //
        // Hash specialization for Key
        //
        template <>
        struct Hash< Key >
        {
            size_t operator() ( const Key & v ) const noexcept { return std::hash< typename Key::ViewT >{}( v.view_ ); }
        };


        //
        // Hash specialization for physical volume
        //
        template <>
        struct Hash< PhysicalVolume >
        {
            size_t operator() ( const PhysicalVolume & v ) const noexcept
            { 
                return std::hash< std::shared_ptr< PhysicalVolumeImpl > >{}( v.impl_.lock() );
            }
        };


        //
        // Provides hash combining constant depending on size of size_t type
        //
        static constexpr size_t hash_constant() noexcept
        {
            static_assert( sizeof( size_t ) == sizeof( uint32_t ) || sizeof( size_t ) == sizeof( uint64_t ), "Cannot detect 32-bit or 64-bit platform" );

            if constexpr ( sizeof( size_t ) == 8 )
            {
                return 0x9E3779B97F4A7C15ULL;
            }
            else
            {
                return 0x9e3779b9U;
            }
        }


        //
        // Provides comined hash value for a variadic sequence of agruments
        //
        template < typename T, typename... Args >
        static auto variadic_hash( const T & v, const Args &... args ) noexcept
        {
            auto seed = variadic_hash( args... );
            return Hash< T >{}( v ) + hash_constant() + ( seed << 6 ) + ( seed >> 2 );
        }


        //
        // Just a terminal specialization of variadic template
        //
        template < typename T >
        static auto variadic_hash( const T & v ) noexcept
        {
            return Hash< T >{}( v );
        }


    private:

        template < typename VolumeT > using VolumeHolderT = std::unordered_set< std::shared_ptr< typename VolumeT::Impl > >;


        template < typename VolumeT >
        static auto singletons()
        {
            static std::mutex guard;
            static VolumeHolderT< VolumeT > holder;
            return std::forward_as_tuple( guard, holder );
        }


        template < typename VolumeT, typename ...Args >
        static std::tuple< RetCode, VolumeT > open( Args&&... args ) noexcept
        {
            try
            {
                auto[ guard, holder ] = singletons< VolumeT >();

                auto pimp = std::make_shared< typename VolumeT::Impl >( std::move( args )... );
                assert( pimp );

                if ( RetCode::Ok != pimp->status() )
                {
                    return { pimp->status(), VolumeT{} };
                }

                std::unique_lock lock( guard );

                if ( auto ok = holder.insert( pimp ).second; !ok )
                {
                    return { RetCode::UnknownError, VolumeT{} };
                }

                return { RetCode::Ok, VolumeT{ pimp } };
            }
            catch ( const std::bad_alloc & )
            {
                return { RetCode::InsufficientMemory, VolumeT{} };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, VolumeT{} };
        }


        template < typename VolumeT >
        static std::tuple< RetCode > close( VolumeT & volume )
        {
            try
            {
                auto[ guard, holder ] = singletons< VolumeT >();

                auto impl = volume.impl_.lock();

                if ( !impl )
                {
                    return { RetCode::InvalidHandle };
                }

                std::unique_lock lock( guard );

                if ( auto it = holder.find( impl ); holder.end() != it )
                {
                    holder.erase( it );
                }
                else
                {
                    return { RetCode::InvalidHandle };
                }

                volume.impl_.reset();

                return { RetCode::Ok };
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }


    public:

        /** Creates new Virtual Volumes

        @retval RetCode - operation sttaus
        @retval VirtualVolume - virtual volume handle
        @throw nothing
        */
        static std::tuple< RetCode, VirtualVolume > OpenVirtualVolume() noexcept
        {
            return open< VirtualVolume >( );
        }


        /** Creates new physical volumes

        @param [in] path - path to a file representing physical starage
        @retval RetCode - operation status
        @retval PhysicalVolume - physical volume handle
        @throw nothing
        */
        static std::tuple< RetCode, PhysicalVolume > OpenPhysicalVolume( const std::filesystem::path & path, size_t priority = 0 ) noexcept
        {
            return open< PhysicalVolume >( std::move( path ), std::move( priority ) );
        }


        /** Closes all oped volumes

        @retval RetCode - operation status
        @throw nothing
        */
        static auto CloseAll( ) noexcept
        {
            try
            {
                auto close_all = [] ( auto singleton ) {
                    auto[ guard, collection ] = singleton;
                    std::scoped_lock lock( guard );
                    collection.clear( );
                };

                close_all( singletons< VirtualVolume >( ) );
                close_all( singletons< PhysicalVolume >( ) );

                return RetCode::Ok;
            }
            catch(...)
            {
            }

            return RetCode::UnknownError;
        }
    };
}


#include "key.h"
#include "virtual_volume.h"
#include "physical_volume.h"
#include "mount_point.h"


#undef _NOEXCEPT_


#endif
