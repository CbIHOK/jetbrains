#ifndef __JB__NODE_LOCKER__H__
#define __JB__NODE_LOCKER__H__


#include <shared_mutex>
#include <unordered_map>
#include <atomic>
#include <list>
#include <functional>
#include <exception>


namespace jb
{
    /** Implements locking of logical path to assure mount consistency

    @tparam Policies - global settings
    @tparam Pad - test pad
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PathLocker
    {
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;

        mutable std::shared_mutex guard_;
        std::unordered_map< NodeUid, std::atomic< size_t > > locked_nodes_;
        RetCode creation_status_ = RetCode::Ok;

        static constexpr auto ExpectedTreeDepth = Policies::PhysicalVolumePolicy::ExpectedTreeDepth;
        static constexpr auto MountPointLimit = Policies::PhysicalVolumePolicy::MountPointLimit;
        static constexpr auto PreallocatedSize = ExpectedTreeDepth * MountPointLimit;
        
    public:

        class PathLock;

        /** Default constructor

        @throw nothing
        */
        PathLocker( ) try : locked_nodes_( PreallocatedSize )
        {
        }
        catch ( const std::bad_alloc & )
        {
            creation_status_ = RetCode::InsufficientMemory;
        }
        catch ( ... )
        {
            creation_status_ = RetCode::UnknownError;
        }


        /** The class is not copyable/movable
        */
        PathLocker( PathLocker&& ) = delete;


        /** Provides object status

        @retval RetCode::Ok if object created successfully, otherwise - error code
        */
        auto creation_status() const noexcept { return creation_status_; }


        /** Gets lock over a key in physical volume

        @param [in] node - uid of the key to be locked (uid of the head B-tree node of a key)
        @retval lock over the key
        @throw may throw std::exception for some reasons
        */
        auto lock_node( NodeUid node )
        {
            std::unique_lock l{ guard_ };

            auto[ it, new_lock ] = locked_nodes_.emplace( node, 1 );

            if ( ! new_lock )
            {
                it->second.fetch_add( 1, std::memory_order_acquire );
            }

            auto unlock = [ = ] ( ) {
                std::unique_lock l{ guard_ };

                if ( it->second.fetch_sub( 1, std::memory_order_release ) - 1 == 0 )
                {
                    locked_nodes_.erase( it );
                }
            };

            return PathLock( unlock );
        }


        /** Checks if a key can be removed

        @param [in] node - uid of the key to be checked
        @retval true if the key can be removed
        @throw may throw std:exception for some reasons
        */
        auto is_removable( NodeUid node ) const
        {
            std::shared_lock l{ guard_ };
            return locked_nodes_.count( node ) == 0;
        }
    };


    /** Holds a lock over a key in physical volume

    @tparam Policies - global settings
    @tparam Pad - test pad
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PathLocker::PathLock
    {
        friend class PathLocker;

        std::list< std::function< void( ) > > unlocks_;


        /** Gets lock over a key

        @param [in] unlock - unlock routine
        @throw may throw std:exception for some reasons
        */
        PathLock( std::function< void( ) > && unlock )
        {
            assert( unlock );
            unlocks_.push_back( unlock );
        }


        /** Unlocks all locked keys

        @throw may throw std:exception for some reasons
        */
        auto unlock_all( )
        {
            std::for_each( rbegin( unlocks_ ), rend( unlocks_ ), [=] ( auto & unlock ) {
                assert( unlock );
                unlock( );
            } );

            unlocks_.clear( );
        }


    public:

        /** Destructor, releases all held lock

        @throw nothing, actually unlock_all() may throw but in this certain case terminate() on
        noexcept gurad looks as a sutable alternative
        */
        ~PathLock() noexcept
        {
            unlock_all();
        }


        /** Default constructor, does not lock anything

        @throw may throw std:exception for some reasons
        */
        PathLock( ) = default;


        /** The class is not copyable
        */
        PathLock( const PathLock & ) = delete;
        PathLock& operator = ( const PathLock & ) = delete;


        /** Move constructor

        Transfers all held locks from origin to newly cleared instance

        @param [in] o - origin as rval
        @throw may throw std:exception for some reasons
        */
        PathLock( PathLock&& o )
        {
            unlocks_ = move( o.unlocks_ );
        }


        /** Move assignment

        Transfers all held locks from origin to the instance, the locks currently assinged for the
        instance are released

        @param [in] o - origin as rval
        @retval the instance as lval
        @throw may throw std:exception for some reasons
        */
        PathLock& operator = ( PathLock&& o )
        {
            unlock_all( );
            unlocks_ = move( o.unlocks_ );
            return *this;
        }


        /** Concatenation operator

        Transfers all held locks from another instance to this

        @param [in] o - other instance
        @retval the instance as lval
        @throw may throw std:exception for some reasons
        */
        PathLock & operator << ( PathLock && o )
        {
            unlocks_.splice( end( unlocks_ ), o.unlocks_ );
            return *this;
        }
    };
}

#endif