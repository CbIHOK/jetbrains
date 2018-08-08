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

#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>


namespace jb
{
    struct DefaultPad {};

    /**
    */
    template < typename Policies, typename Pad = DefaultPad >
    class Storage
    {
        friend typename Pad;

    public:

        /** Enumerates all possible return codes
        */
        enum class RetCode
        {
            Ok,                     ///< Operation succedded
            UnknownError,           ///< Something wrong happened
            InsufficientMemory,     ///< Operation failed due to low memory
            InvalidHandle,          ///< Given handle does not address valid object
            MountPointLimitReached, ///< Virtual Volume already has maximum number of Mounts Points
            AlreadyExists,          ///< Such Mount Point already exist
            InvalidLogicalKey,
            InvalidPhysicalKey,
        };


        /** Virtual volume type
        */
        class VirtualVolume;
        class PhysicalVolume;
        class MountPoint;


    private:

        template <typename T> struct SingletonPolicy {};

        template <>
        struct SingletonPolicy< VirtualVolume >
        {
            static constexpr size_t Limit = Policies::VirtualVolumePolicy::VolumeLimit;
            using Impl = typename VirtualVolume::Impl;
            using ImplP = std::shared_ptr< Impl >;
            using CollectionT = std::unordered_set< ImplP >;
            using MutexT = std::mutex;
        };
        
        template <> 
        struct SingletonPolicy< PhysicalVolume >
        {
            static constexpr size_t Limit = Policies::PhysicalVolumePolicy::VolumeLimit;
            using Impl = typename PhysicalVolume::Impl;
            using ImplP = std::shared_ptr< Impl >;
            using CollectionT = std::unordered_map< ImplP, size_t >;
            using MutexT = std::shared_mutex;
        };

        template < typename T >
        static auto singletons()
        {
            using Policy = SingletonPolicy< T >;
            // c++x guaranties thread safe initialization of the static variables
            static typename Policy::MutexT mutex;
            static typename Policy::CollectionT holder( Policy::Limit );
            return std::forward_as_tuple( mutex, holder );
        }


        template < typename T >
        static auto update_physical_volume_priorities( T && update_lambda )
        {
            using Impl = PhysicalVolume::Impl;
            using ImplP = std::shared_lock< Impl >;

            auto volume = pv.lock();

            if ( !volume )
            {
                return RetCode::InvalidHandle;
            }

            auto[ guard, collection ] = physical_volume_singletons();

            std::unique_lock<std::shared_mutex> lock( guard );

            if ( auto i = collection.find( volume ); i != collection.end() )
            {
                auto priority = i->second;
                for ( auto && v : collection ) update_lambda( v );
            }
            else
            {
                return RetCode::UnknownError;
            }
        }

        static auto physical_volume_prioritize_on_bottom( PhysicalVolume pv ) {}
        static auto physical_volume_prioritize_before( PhysicalVolume pv, PhysicalVolume before ) {}
        static auto physical_volume_prioritize_after( PhysicalVolume pv, PhysicalVolume after ) {}

        /* Helper, closes given handler

        Invalidates given volume handler, destroys associated Virtual Volume object, and release
        allocated resources. If there are operations locking the volume, the function postpones
        the actions until all locking operations get completed. Unlocked volume is destroyed
        immediately.

        @tparam T - volume type, auto deducing implied
        @param [in] volume - volume to be closed

        @return std::tuple< ret_code >
        ret_code == RetCode::Ok => operation succedded
        ret_code == RetCode::InvalidVolumeHandle => passed handle does not refer a volume
        ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        */
        template< typename T >
        static auto close(T * volume) noexcept
        {
            assert(volume);

            using ValidVolumeTypes = boost::mpl::vector< VirtualVolume, PhysicalVolume >;
            static_assert(boost::mpl::contains< ValidVolumeTypes, T >::type::value, "Invalid volume type");

            try
            {
                auto[guard, collection] = singletons< T >();
                {
                    std::scoped_lock lock( guard );

                    auto impl = volume->impl_.lock();

                    if (!impl)
                    {
                        return RetCode::InvalidHandle;
                    }
                    else if (auto i = collection.find(impl); i != collection.end())
                    {
                        collection.erase(i);
                        return RetCode::Ok;
                    }
                    else
                    {
                        return RetCode::InvalidHandle;
                    }
                }
            }
            catch (...)
            {
            }

            return RetCode::UnknownError;
        }


    public:

        /** Creates new Virtual Volumes

        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of Virtual Volume, possible return codes are
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::InsufficientMemory => operation failed due to low memory
            ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        */
        [[nodiscard]]
        static auto OpenVirtualVolume() noexcept
        {
            try
            {
                auto[ guard, collection ] = singletons< VirtualVolume >();

                std::scoped_lock lock( guard );

                auto impl = std::make_shared< VirtualVolume::Impl>();

                if ( auto[ i, success ] = collection.insert( impl ); success )
                {
                    return std::pair{ RetCode::Ok, VirtualVolume( *i ) };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return std::pair{ RetCode::InsufficientMemory, VirtualVolume() };
            }
            catch(...)
            {
            }

            return std::pair{ RetCode::UnknownError, VirtualVolume() };
        }


        /** Creates new physical volumes

        @param [in] path - path to a file representing physical starage
        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of physical volume, possible return codes are
           ret_code == RetCode::Ok => operation succedded
           ret_code == RetCode::InsufficientMemory => operation failed due to low memory
           ret_code == RetCode::FileNotFound => given file path leads to nowhere
           ret_code == RetCode::FileAlreadyOpened => given file is already opened by this process
           ret_code == RetCode::FileIsLocked => given file is locked by another process
           ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        @todo implement
        */
        [[nodiscard]]
        static auto OpenPhysicalVolume( std::filesystem::path && path ) noexcept
        {
            try
            {
                auto[ guard, collection ] = singletons< PhysicalVolume >();

                std::scoped_lock lock( guard );

                using Impl = PhysicalVolume::Impl;

                auto[ it, success ] = collection.insert( { 
                    std::make_shared< Impl>(), 
                    std::numeric_limits<size_t>::max() 
                } );
                
                if (success )
                {
                    return std::pair{ RetCode::Ok, VirtualVolume( it->first ) };
                }
            }
            catch ( const std::bad_alloc & )
            {
                return std::pair{ RetCode::InsufficientMemory, VirtualVolume() };
            }
            catch ( ... )
            {
            }

            return std::pair{ RetCode::UnknownError, VirtualVolume() };
        }
    };
}


#include "virtual_volume.h"
#include "physical_volume.h"
#include "mount_point.h"


#endif
