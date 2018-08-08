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

       
        template < typename VolumeT >
        struct SingletonPolicy
        {
            static constexpr auto enabled = false;
        };


        template <>
        struct SingletonPolicy< VirtualVolume >
        {
            static constexpr auto enabled = true;

            static constexpr size_t Limit = Policies::VirtualVolumePolicy::VolumeLimit;

            using ImplT = typename VirtualVolume::Impl;
            using ImplP = typename std::shared_ptr< ImplT >;
            using CollectionT = std::unordered_set< ImplP >;
            using MutexT = std::mutex;

            template< typename ...Arg >
            static constexpr auto CreatorF( Arg&&... args )
            {
                return std::make_shared< ImplT >( std::forward( args )... );
            }

            static constexpr auto InserterF = [] ( CollectionT && collection, const ImplP & item )
            {
                return collection.insert( item ).second;
            };

            static constexpr auto DeleterF = [] ( CollectionT && collection, const ImplP & item )
            {
                if ( auto it = collection.find( item ); it != collection.end( ) )
                {
                    collection.erase( it );
                    return true;
                }

                return false;
            };
        };


        template <> 
        struct SingletonPolicy< PhysicalVolume >
        {
            static constexpr auto enabled = true;

            static constexpr size_t Limit = Policies::PhysicalVolumePolicy::VolumeLimit;

            using ImplT = typename PhysicalVolume::Impl;
            using ImplP = typename std::shared_ptr< ImplT >;
            using CollectionT = std::unordered_map< ImplP, size_t >;
            using MutexT = std::shared_mutex;

            template< typename ...Arg >
            static constexpr auto CreatorF( Arg&&... args )
            {
                return std::make_shared< ImplT >( std::forward( args )... );
            }

            static constexpr auto InserterF = []( CollectionT && collection, const ImplP & item )
            {
                return collection.insert( { item, std::numeric_limits< size_t >::max() } ).second;
            };

            static constexpr auto DeleterF = [] ( CollectionT && collection, const ImplP & item )
            {
                if ( auto it = collection.find( item ); it != collection.end() )
                {
                    collection.erase( it );
                    return true;
                }

                return false;
            };
        };


        template < typename VolumeT >
        [ [ nodiscard ] ]
        static auto singletons()
        {
            using SingletonPolicy = SingletonPolicy< VolumeT >;
            static_assert( SingletonPolicy::enabled, "Unsupported volume type");

            // c++x guaranties thread safe initialization of the static variables
            static typename SingletonPolicy::MutexT mutex;
            static typename SingletonPolicy::CollectionT holder( SingletonPolicy::Limit );

            return std::forward_as_tuple( mutex, holder );
        }


        template < typename VolumeT, typename ...Args >
        [ [ nodiscard ] ]
        static auto open( Args&&... args) noexcept
        {
            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                // get singletons
                auto[ guard, collection ] = singletons< VolumeT >();
                std::scoped_lock lock( guard );

                // create new item and add it into collection
                auto item = SingletonPolicy::CreatorF( std::forward(args)... );
                
                if ( SingletonPolicy::InserterF( std::move(collection), item ) )
                {
                    return std::pair{ RetCode::Ok, VolumeT( item ) };
                }
            }
            catch (const std::bad_alloc &)
            {
                return std::pair{ RetCode::InsufficientMemory, VolumeT() };
            }
            catch (...)
            {
            }
            
            return std::pair{ RetCode::UnknownError, VolumeT() };
        }

        template < typename VolumeT >
        [ [ nodiscard ] ]
        static auto close( VolumeT && volume )
        {
            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                auto[ guard, collection ] = singletons< VolumeT >( );
                std::scoped_lock lock( guard );

                auto impl = std::move( volume.impl_ );
                auto item = impl.lock( );

                if ( !item )
                {
                    return RetCode::InvalidHandle;
                }
                else if ( SingletonPolicy::DeleterF( std::move(collection), item ) )
                {
                    return RetCode::Ok;
                }
                else
                {
                    return RetCode::InvalidHandle;
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }

        template < typename T >
        static auto update_physical_volume_priorities( T && labmda )
        {
            auto[ guard, collection ] = singletons< PhysicalVolume >();
            std::unique_lock< std::shared_mutex > lock( guard );

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

        static auto physical_volume_prioritize_on_bottom( PhysicalVolume && volume )
        {
            auto[ guard, collection ] = singletons< PhysicalVolume >( );
            std::unique_lock< std::shared_mutex > lock( guard );
        }

        static auto physical_volume_prioritize_on_bottom( PhysicalVolume * pv ) {}
        static auto physical_volume_prioritize_before( PhysicalVolume * pv, PhysicalVolume * before ) {}
        static auto physical_volume_prioritize_after( PhysicalVolume * pv, PhysicalVolume * after ) {}


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
            return open< VirtualVolume >( );
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

                auto[ ret, handle ] = open< PhysicalVolume >( );

                if ( RetCode::Ok == ret )
                {
                    physical_volume_prioritize_on_bottom( handle );
                }

                return std::pair{ ret, handle };
            }
            catch ( ... )
            {
            }

            return std::pair{ RetCode::UnknownError, PhysicalVolume() };
        }
    };
}


#include "virtual_volume.h"
#include "physical_volume.h"
#include "mount_point.h"


#endif
