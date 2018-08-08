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


    template < typename T > struct Hash { static constexpr bool enabled = false; };


    template < typename Policies, typename Pad > class VirtualVolume;
    template < typename Policies, typename Pad > class PhysicalVolume;
    template < typename Policies, typename Pad > class MountPoint;


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


    /**
    */
    template < typename Policies, typename Pad = DefaultPad >
    class Storage
    {
        using VirtualVolume  = ::jb::VirtualVolume< Policies, Pad >;
        using PhysicalVolume = ::jb::PhysicalVolume< Policies, Pad >;
        using MountPoint     = ::jb::MountPoint< Policies, Pad >;

        friend typename Pad;

     
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
            using CollectionT = std::unordered_map< ImplP, int >;
            using MutexT = std::shared_mutex;

            template< typename ...Arg >
            static constexpr auto CreatorF( Arg&&... args )
            {
                return std::make_shared< ImplT >( std::forward( args )... );
            }

            static constexpr auto InserterF = []( CollectionT && collection, const ImplP & item )
            {
                return collection.insert( { item, std::numeric_limits< int >::max() } ).second;
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
        [[ nodiscard ]]
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
        [[ nodiscard ]]
        static auto open( Args&&... args) noexcept
        {
            using namespace std;

            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                // get singletons
                auto[ guard, collection ] = singletons< VolumeT >();
                scoped_lock lock( guard );

                // create new item and add it into collection
                auto item = SingletonPolicy::CreatorF( std::forward(args)... );
                
                if ( SingletonPolicy::InserterF( move(collection), item ) )
                {
                    return pair{ RetCode::Ok, VolumeT( item ) };
                }
            }
            catch (const bad_alloc &)
            {
                return pair{ RetCode::InsufficientMemory, VolumeT() };
            }
            catch (...)
            {
            }
            
            return pair{ RetCode::UnknownError, VolumeT() };
        }

        template < typename VolumeT >
        [[nodiscard]]
        static auto close( const VolumeT & volume )
        {
            using namespace std;

            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                auto[ guard, collection ] = singletons< VolumeT >( );
                scoped_lock lock( guard );

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

        [[ nodiscard ]]
        static auto prioritize_on_top( const PhysicalVolume & volume ) noexcept
        {
            using namespace std;

            try
            {
                auto[ guard, collection ] = singletons< PhysicalVolume >( );
                unique_lock< shared_mutex > lock( guard );

                if ( auto volume_it = collection.find( volume.impl_.lock( ) ); volume_it != collection.end( ) )
                {
                    auto volume_priority = volume_it->second;

                    for_each( execution::par, begin( collection ), end( collection ), [] (it ) {
                        if ( it == volume_it )
                        {
                            it->second = 0;
                        }
                        else if ( it->second < volume_priority )
                        {
                            it->second += 1;
                        }
                    } );

                    return RetCodeOk::Ok;
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

        [[ nodiscard ]]
        static auto prioritize_on_bottom( const PhysicalVolume & volume ) noexcept
        {
            using namespace std;

            try
            {
                auto[ guard, collection ] = singletons< PhysicalVolume >( );
                unique_lock< shared_mutex > lock( guard );

                if ( auto volume_it = collection.find( volume.impl_.lock( ) ); volume_it != collection.end( ) )
                {
                    auto volume_priority = volume_it->second;

                    for_each( execution::par, begin( collection ), end( collection ), [] ( it ) {
                        if ( it == volume_it )
                        {
                            it->second = collection.size( ) - 1;
                        }
                        else if ( it->second > volume_priority )
                        {
                            it->second -= 1;
                        }
                    } );

                    return RetCodeOk::Ok;
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


        [[ nodiscard ]]
        static auto prioritize_before( const PhysicalVolume & volume, const PhysicalVolume & before )
        {
            using namespace std;

            try
            {
                auto[ guard, collection ] = singletons< PhysicalVolume >( );
                unique_lock< shared_mutex > lock( guard );

                if ( auto volume_it = collection.find( volume.impl_.lock() ); volume_it != collection.end )
                {
                    auto volume_priority = volume_it->second;

                    if ( auto before_it = collection.find( before.impl_.lock() ); before_it != collection.end() )
                    {
                        auto before_priority = before_it->second;
                        
                        auto[ lower, upper, increment, set_volume, set_before ] =
                            ( volume_priority < before_priority ) ?
                            tuple{ volume_priority + 1, before_priority, -1, before_priority - 1, before_priority } :
                            tuple{ before_priority + 1, volume_priority,  1, before_priority, before_priority + 1 };

                        for_each( execution::par, begin( collection ), end( collection ), []( it ){
                            if ( it == volume_it )
                            {
                                it->second = set_volume;
                            }
                            else if ( it == before_it )
                            {
                                it->second = set_before;
                            }
                            else if ( lower <= it->second && it->second < upper )
                            {
                                it->second += increment;
                            }
                        } );

                        return RetCodeOk::Ok;
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
            }

            return RetCode::UnknownError;
        }


        [[ nodiscard ]]
        static auto prioritize_after( const PhysicalVolume & volume, const PhysicalVolume & after )
        {
            using namespace std;

            try
            {
                auto[ guard, collection ] = singletons< PhysicalVolume >( );
                unique_lock< shared_mutex > lock( guard );

                if ( auto volume_it = collection.find( volume.impl_.lock( ) ); volume_it != collection.end() )
                {
                    auto volume_priority = volume_it->second;

                    if ( auto after_it = collection.find( after.impl_.lock( ) ); after_it != collection.end() )
                    {
                        auto after_priority = after_it->second;

                        auto[ lower, upper, increment, set_volume, set_after ] =
                            ( volume_priority < after_priority ) ?
                            tuple{ volume_priority + 1, after_priority,  -1, after_priority,     after_priority - 1  } :
                            tuple{ after_priority + 1,  volume_priority,  1, after_priority + 1, after_priority      };

                        for_each( execution::par, begin( collection ), end( collection ), [] ( it ) {
                            if ( it == volume_it )
                            {
                                it->second = set_volume;
                            }
                            else if ( it == before_it )
                            {
                                it->second = set_before;
                            }
                            else if ( lower <= it->second && it->second < upper )
                            {
                                it->second += increment;
                            }
                        } );

                        return RetCodeOk::Ok;
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
                // lock over both open() and prioritize_on_bottom()
                auto[ guard, collection ] = singletons< PhysicalVolume >();
                std::unique_lock< std::shared_mutex > lock( guard );

                if ( auto[ ret, volume ] = open< PhysicalVolume >( );  RetCode::Ok == ret )
                {
                    assert( volume );

                    if ( auto ret != prioritize_on_bottom( volume ) )
                    {
                        return std::pair{ RetCode::Ok, volume };
                    }
                    else
                    {
                        assert( close( volume ) != RetCode::InvalidVolume );
                        return std::pair{ ret, volume };
                    }
                }
                else
                {
                    return std::pair{ ret, volume };
                }
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
