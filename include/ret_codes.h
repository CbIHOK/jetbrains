#ifndef __JB__RET_CODES__H__
#define __JB__RET_CODES__H__

namespace jb
{
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
}

#endif