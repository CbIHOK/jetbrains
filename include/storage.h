#ifndef __JB__STORAGE__H__
#define __JB__STORAGE__H__


#include <mutex>
#include <unordered_set>
#include <tuple>
#include <filesystem>
#include <memory>


namespace jb
{

    template < typename Policies > class VirtualVolume;
    template < typename Policies > class PhysicalVolume;


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

    static constexpr auto Ok = RetCode::Ok;
    static constexpr auto InvalidHandle = RetCode::InvalidHandle;
    static constexpr auto InvalidVirtualVolume = RetCode::InvalidVirtualVolume;
    static constexpr auto InvalidPhysicalVolume = RetCode::InvalidPhysicalVolume;
    static constexpr auto InvalidMountPoint = RetCode::InvalidMountPoint;
    static constexpr auto VolumeAlreadyMounted = RetCode::VolumeAlreadyMounted;
    static constexpr auto InvalidKey = RetCode::InvalidKey;
    static constexpr auto InvalidMountAlias = RetCode::InvalidMountAlias;
    static constexpr auto InvalidLogicalPath = RetCode::InvalidLogicalPath;
    static constexpr auto InvalidPhysicalPath = RetCode::InvalidPhysicalPath;
    static constexpr auto PathLocked = RetCode::PathLocked;
    static constexpr auto NotFound = RetCode::NotFound;
    static constexpr auto InUse = RetCode::InUse;
    static constexpr auto HasDependentMounts = RetCode::HasDependentMounts;
    static constexpr auto MaxTreeDepthExceeded = RetCode::MaxTreeDepthExceeded;
    static constexpr auto SubkeyLimitReached = RetCode::SubkeyLimitReached;
    static constexpr auto AlreadyExpired = RetCode::AlreadyExpired;
    static constexpr auto AlreadyExists = RetCode::AlreadyExists;
    static constexpr auto NotLeaf = RetCode::NotLeaf;
    static constexpr auto IncompatibleFile = RetCode::IncompatibleFile;
    static constexpr auto AlreadyOpened = RetCode::AlreadyOpened;
    static constexpr auto UnableToOpen = RetCode::UnableToOpen;
    static constexpr auto TooManyConcurrentOps = RetCode::TooManyConcurrentOps;
    static constexpr auto IoError = RetCode::IoError;
    static constexpr auto InvalidData = RetCode::InvalidData;
    static constexpr auto InsufficientMemory = RetCode::InsufficientMemory;
    static constexpr auto UnknownError = RetCode::UnknownError;
    static constexpr auto NotYetImplemented = RetCode::NotYetImplemented;


    struct Implemenation
    {
        using 
    };

    /**
    */
    template < typename Policies, typename TestPad = Implemenation >
    class Storage
    {
        using VirtualVolumeT = VirtualVolume< Policies >;
        using PhysicalVolumeT = PhysicalVolume< Policies >;


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

        static std::tuple < RetCode, std::weak_ptr< VirtualVolumeT > > open_virtual_volume() noexcept
        {
            return open< VirtualVolumeT >();
        }

        static std::tuple < RetCode, std::weak_ptr< PhysicalVolumeT > > open_physical_volume( const std::filesystem::path & path, size_t priority = 0 ) noexcept
        {
            return open< PhysicalVolumeT >( path, priority );
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
                    auto[ guard, holder ] = singletons< VirtualVolumeT >();
                    std::unique_lock lock( guard );
                    holder.clear();
                }
                {
                    auto[ guard, holder ] = singletons< PhysicalVolumeT >();
                    std::unique_lock lock( guard );
                    holder.clear();
                }

                return Ok;
            }
            catch(...)
            {
                return UnknownError;
            }
        }
    };
}


#endif
