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
class TestStorageFile;
class TestBTree;

namespace jb
{

    struct DefaultPad {};


    /**
    */
    template < typename Policies, typename Pad >
    class Storage
    {
    public:

        template < typename T >
        struct Hash
        {
            size_t operator() ( const T & v ) const noexcept { return std::hash< T >{}( v ); }
        };

    private:

        template < typename T > friend struct Hash;

        friend typename Pad;
        friend class TestStorage;
        friend class TestKey;
        friend class TestNodeLocker;
        friend class TestBloom;
        friend class TestStorageFile;

        friend class VirtualVolume;
        friend class PhysicalVolume;
        friend class MountPoint;

        class Key;
        class VirtualVolumeImpl;
        class PhysicalVolumeImpl;
        class MountPointImpl;

        using KeyValue = typename Key::ValueT;

    public:

        /** Enumerates all possible return codes
        */
        enum class RetCode
        {
            Ok,                     ///< Operation succedded
            UnknownError,           ///< Something wrong happened
            InsufficientMemory,     ///< Operation failed due to low memory
            InvalidHandle,          ///< Given handle does not address valid object
            LimitReached,           ///< All handles of a type are alreay exhausted
            VolumeAlreadyMounted,   ///< Attempt to mount the same physical volume at the same logical path
            InvalidKey,             ///< Invalid key value
            InvalidSubkey,          ///< Invalid subkey value
            InvalidLogicalPath,     ///< Given logical path cannot be mapped onto a physical one
            NotFound,               ///< Such path does not have a physical representation
            InUse,                  ///< The handler is currently used by concurrent operation and cannot be closed
            HasDependentMounts,     ///< There are underlaying mount
            TooManyConcurrentOps,   ///< The limit of concurent operations over physical volume is reached
            MaxTreeDepthExceeded,   ///< Cannot search such deep inside
            AlreadyExpired,         ///< Given timestamp already in the past
            KeyAlreadyExists,       ///< Key already exists
            AlreadyOpened,          ///< Physical file is already opened
            UnableToOpen,           ///< Cannot open specified file
            UnableToCreate,         ///< Unable to create file of a name
            IoError,                ///< General I/O error
            IncompatibleFile,       ///< File is incompatible
            ///
            NotYetImplemented
        };

        using Value = typename Policies::ValueT;
        using Timestamp = std::filesystem::file_time_type;

        class VirtualVolume;
        class PhysicalVolume;
        class MountPoint;


        template <>
        struct Hash< Key >
        {
            size_t operator() ( const Key & v ) const noexcept { return std::hash< typename Key::ViewT >{}( v.view_ ); }
        };

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
            static_assert( sizeof( size_t ) == 8 || sizeof( size_t ) == 4, "Cannot detect 32-bit or 64-bit platform" );

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
                return collection.insert( { item, std::numeric_limits< int >::max() } ).second;
            };

            static constexpr auto DeleterF = [] ( CollectionT & collection, const ImplP & item )
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
            static typename SingletonPolicy::CollectionT holder{ SingletonPolicy::Limit };

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

                if ( collection.size( ) >= SingletonPolicy::Limit )
                {
                    return pair{ RetCode::LimitReached, VolumeT{} };
                }

                // create new item and add it into collection
                auto item = SingletonPolicy::CreatorF( args... );
                
                if ( SingletonPolicy::InserterF( collection, item ) )
                {
                    return pair{ RetCode::Ok, VolumeT{ item } };
                }
            }
            catch (const bad_alloc &)
            {
                return pair{ RetCode::InsufficientMemory, VolumeT{} };
            }
            catch (...)
            {
            }
            
            return pair{ RetCode::UnknownError, VolumeT{} };
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

                auto impl = volume.impl_.lock( );

                if ( ! impl )
                {
                    return RetCode::InvalidHandle;
                }
                else if ( SingletonPolicy::DeleterF( collection, impl ) )
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

                    for_each( execution::par, begin( collection ), end( collection ), [=] ( auto & pv ) {
                        if ( pv.second == volume_priority )
                        {
                            pv.second = 0;
                        }
                        else if ( pv.second < volume_priority )
                        {
                            pv.second += 1;
                        }
                    } );

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

                    for_each( execution::par, begin( collection ), end( collection ), [=] ( auto & pv ) {
                        if ( pv.second == volume_priority )
                        {
                            pv.second = static_cast< int >( collection.size() ) - 1;
                        }
                        else if ( pv.second > volume_priority )
                        {
                            pv.second -= 1;
                        }
                    } );

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
        static auto prioritize_before( const PhysicalVolume & volume, const PhysicalVolume & before )
        {
            using namespace std;

            try
            {
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

                        for_each( execution::par, begin( collection ), end( collection ), [=]( auto & pv ){
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

                        for_each( execution::par, begin( collection ), end( collection ), [=] ( auto & pv ) {
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
            }

            return RetCode::UnknownError;
        }


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


        [[nodiscard]]
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
        static auto OpenPhysicalVolume( const std::filesystem::path & path, bool create = false ) noexcept
        {
            using namespace std;
            try
            {
                // lock over both open() and prioritize_on_bottom()
                static mutex mtx;
                scoped_lock lock( mtx );

                if ( auto[ ret, volume ] = open< PhysicalVolume >( filesystem::absolute( path ), create );  RetCode::Ok == ret )
                {
                    assert( volume );

                    if ( auto ret = prioritize_on_bottom( volume ); ret == RetCode::Ok )
                    {
                        return std::pair{ RetCode::Ok, volume };
                    }
                    else
                    {
                        assert( RetCode::InvalidHandle != close( volume ) );
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
