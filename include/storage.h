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


class TestStorage;
class TestKey;
class TestNodeLocker;
class TestBloom;
class TestPackedValue;
template < typename T > class TestStorageFile;
template < typename T > class TestBTree;

namespace jb
{

    struct DefaultPad {};


    /**
    */
    template < typename Policies >
    class Storage
    {

    public:

        //
        //
        //
        template < typename T >
        struct Hash
        {
            size_t operator() ( const T & v ) const noexcept { return std::hash< T >{}( v ); }
        };


    private:

        template < typename T > friend struct Hash;

        friend class TestStorage;
        friend class TestKey;
        friend class TestNodeLocker;
        friend class TestBloom;
        friend class TestPackedValue;
        template < typename T > friend class TestStorageFile;
        template < typename T > friend class TestBTree;

        friend class VirtualVolume;
        friend class PhysicalVolume;
        friend class MountPoint;

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
            InvalidData,
            InsufficientMemory,     ///< Operation failed due to low memory
            UnknownError,           ///< Something wrong happened
                                    ///
            NotYetImplemented
        };

        using KeyValue = typename Key::ValueT;
        using Value = typename Policies::ValueT;


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

        //
        // declares singletons policy for volume type
        //
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

            using ImplT = typename VirtualVolumeImpl;
            using ImplP = typename std::shared_ptr< ImplT >;
            using CollectionT = std::unordered_set< ImplP >;
            using MutexT = std::mutex;

            template< typename ...Arg >
            static constexpr auto CreatorF( Arg&&... args )
            {
                return std::make_shared< ImplT >( std::forward( args )... );
            }

            static constexpr auto InserterF = [] ( CollectionT & collection, const ImplP & item )
            {
                return collection.insert( item ).second;
            };

            static constexpr auto DeleterF = [] ( CollectionT & collection, const ImplP & item )
            {
                if ( auto it = collection.find( item ); it != collection.end( ) )
                {
                    collection.erase( it );
                }
                else
                {
                    throw std::logic_error( "Unknown element" );
                }
            };
        };


        template <> 
        struct SingletonPolicy< PhysicalVolume >
        {
            static constexpr auto enabled = true;

            static constexpr size_t Limit = Policies::PhysicalVolumePolicy::VolumeLimit;

            using ImplT = typename PhysicalVolumeImpl;
            using ImplP = typename std::shared_ptr< ImplT >;
            using CollectionT = std::unordered_map< ImplP, int >;
            using MutexT = std::shared_mutex;

            template< typename ...Arg >
            static constexpr auto CreatorF( Arg&&... args )
            {
                return std::make_shared< ImplT >( move(args)... );
            }

            static constexpr auto InserterF = []( CollectionT & collection, const ImplP & item )
            {
                if ( !collection.insert( { item, std::numeric_limits< int >::max() } ).second )
                {
                    throw std::runtime_error( "Unable to store PIMP" );
                }
            };

            static constexpr auto DeleterF = [] ( CollectionT & collection, const ImplP & item )
            {
                if ( auto it = collection.find( item ); it != collection.end() )
                {
                    collection.erase( it );
                }
                else
                {
                    throw std::logic_error( "Unknown element" );
                }
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
            static typename SingletonPolicy::CollectionT holder{ SingletonPolicy::Limit };

            return std::forward_as_tuple( mutex, holder );
        }


        template < typename VolumeT, typename ...Args >
        [[ nodiscard ]]
        static std::tuple< RetCode, VolumeT > open( Args&&... args) noexcept
        {
            using namespace std;

            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                // get singletons
                auto[ guard, collection ] = singletons< VolumeT >();
                scoped_lock lock( guard );

                if ( collection.size( ) >= SingletonPolicy::Limit )
                {
                    return { RetCode::LimitReached, VolumeT{} };
                }

                // create new item and add it into collection
                auto item = SingletonPolicy::CreatorF( args... );
                SingletonPolicy::InserterF( collection, item );

                return { RetCode::Ok, VolumeT{ item } };
            }
            catch (const bad_alloc &)
            {
                return { RetCode::InsufficientMemory, VolumeT{} };
            }
            catch (...)
            {
            }
            
            return { RetCode::UnknownError, VolumeT{} };
        }


        //
        // closes given volume handle
        //
        template < typename VolumeT >
        [[nodiscard]]
        static auto close( const VolumeT & volume ) noexcept
        {
            using namespace std;

            try
            {
                using SingletonPolicy = SingletonPolicy< VolumeT >;
                static_assert( SingletonPolicy::enabled, "Unsupported volume type" );

                auto[ guard, collection ] = singletons< VolumeT >( );
                scoped_lock lock( guard );

                auto impl = volume.impl_.lock( );

                if ( ! impl )
                {
                    return RetCode::InvalidHandle;
                }
                else if ( impl.use_count() - 1 > 1 )
                {
                    return RetCode::InUse;
                }
                
                SingletonPolicy::DeleterF( collection, impl );
                return RetCode::Ok;
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        //
        // assign volume with the highest priority
        //
        static auto prioritize_on_top( const PhysicalVolume & volume )
        {
            using namespace std;

            auto[ guard, collection ] = singletons< PhysicalVolume >( );
            unique_lock< shared_mutex > lock( guard );

            if ( auto volume_it = collection.find( volume.impl_.lock( ) ); volume_it != collection.end( ) )
            {
                auto volume_priority = volume_it->second;

                // explicitly prevent parallelism cuz it will cause a lot of cache misses
                for_each( execution::seq, begin( collection ), end( collection ), [=] ( auto & pv ) {
                    if ( pv.second == volume_priority )
                    {
                        pv.second = 0;
                    }
                    else if ( pv.second < volume_priority )
                    {
                        pv.second += 1;
                    }
                } );
            }

            throw std::logic_error( "Unable to find PIMP" );
        }


        //
        // assign volume with the lowest priority
        //
        static auto prioritize_on_bottom( const PhysicalVolume & volume ) 
        {
            using namespace std;

            auto[ guard, collection ] = singletons< PhysicalVolume >( );
            unique_lock< shared_mutex > lock( guard );

            if ( auto volume_it = collection.find( volume.impl_.lock( ) ); volume_it != collection.end( ) )
            {
                auto volume_priority = volume_it->second;

                for_each( execution::seq, begin( collection ), end( collection ), [=] ( auto & pv ) {
                    if ( pv.second == volume_priority )
                    {
                        pv.second = static_cast< int >( collection.size() ) - 1;
                    }
                    else if ( pv.second > volume_priority )
                    {
                        pv.second -= 1;
                    }
                } );
            }

            throw std::logic_error( "Unable to find PIMP" );
        }


        //
        // prioritizes the volume above given one
        //
        static auto prioritize_before( const PhysicalVolume & volume, const PhysicalVolume & before )
        {
            using namespace std;

            auto[ guard, collection ] = singletons< PhysicalVolume >( );
            unique_lock< shared_mutex > lock( guard );

            if ( auto volume_it = collection.find( volume.impl_.lock() ); volume_it != collection.end() )
            {
                auto volume_priority = volume_it->second;

                if ( auto before_it = collection.find( before.impl_.lock() ); before_it != collection.end() )
                {
                    auto before_priority = before_it->second;
                        
                    auto[ lower, upper, increment, set_volume, set_before ] =
                        ( volume_priority < before_priority ) ?
                        tuple{ volume_priority + 1, before_priority, -1, before_priority - 1, before_priority } :
                        tuple{ before_priority + 1, volume_priority,  1, before_priority, before_priority + 1 };

                    for_each( execution::seq, begin( collection ), end( collection ), [=] ( auto & pv ) {
                        if ( pv.second == volume_priority )
                        {
                            pv.second = set_volume;
                        }
                        else if ( pv.second == before_priority )
                        {
                            pv.second = set_before;
                        }
                        else if ( lower <= pv.second && pv.second < upper )
                        {
                            pv.second += increment;
                        }
                    } );
                }
            }

            throw std::logic_error( "Unable to find PIMP" );
        }


        //
        // prioritizes the volume below given one
        //
        static auto prioritize_after( const PhysicalVolume & volume, const PhysicalVolume & after )
        {
            using namespace std;

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

                    for_each( execution::seq, begin( collection ), end( collection ), [=] ( auto & pv ) {
                        if ( pv.second == volume_priority )
                        {
                            pv.second = set_volume;
                        }
                        else if ( pv.second == after_priority )
                        {
                            pv.second = set_after;
                        }
                        else if ( lower <= pv.second && pv.second < upper )
                        {
                            pv.second += increment;
                        }
                    } );
                }
            }

            throw std::logic_error( "Unable to find PIMP" );
        }


        //
        // physical volume priority comparer
        //
        struct lesser_priority : std::shared_lock< std::shared_mutex >
        {
            friend class TestStorage;

            using LockT = std::shared_lock< std::shared_mutex >;
            using CollectionT = typename SingletonPolicy< PhysicalVolume >::CollectionT;
            using ImplP = typename SingletonPolicy< PhysicalVolume >::ImplP;

            lesser_priority() noexcept = default;

            lesser_priority( const lesser_priority & ) = delete;
            lesser_priority & operator = ( const lesser_priority & ) = delete;

            lesser_priority( lesser_priority && ) noexcept = default;
            lesser_priority & operator = ( lesser_priority && ) noexcept = default;
            
            lesser_priority( LockT && lock, const CollectionT & collection ) noexcept
                : LockT( std::move(lock) )
                , collection_( collection )
                , valid_( true )
            {}

            bool operator () ( const ImplP & l, const ImplP & r ) const
            {
                assert( valid_ && l && r );

                auto l_it = collection_.find( l );
                auto r_it = collection_.find( r );
                
                assert( l_it != collection_.end( ) && r_it != collection_.end( ) );
                
                return l_it->second < r_it->second;
            }

        private:

            bool valid_ = false;
            const CollectionT & collection_;

        };


        //
        // locks collection of physical volume and provide physical volume priority comparer object
        //
        static auto get_lesser_priority()
        {
            using namespace std;
            
            using CollectionT = typename SingletonPolicy< PhysicalVolume >::CollectionT;

            auto[ guard, collection ] = singletons< PhysicalVolume >( );
            shared_lock< shared_mutex > lock( guard );

            return lesser_priority{ move( lock ), collection };
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
        static std::tuple< RetCode, PhysicalVolume > OpenPhysicalVolume( const std::filesystem::path & path ) noexcept
        {
            using namespace std;
            try
            {
                // lock over both open() and prioritize_on_bottom()
                static mutex mtx;
                scoped_lock lock( mtx );

                if ( auto[ ret, volume ] = open< PhysicalVolume >( filesystem::absolute( path ) );  RetCode::Ok == ret )
                {
                    prioritize_on_bottom( volume );
                    return { volume.impl_.lock()->status(), volume };
                }
                else
                {
                    return { ret, volume };
                }
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, PhysicalVolume() };
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


#endif
