#ifndef __JB__NODE_LOCKER__H__
#define __JB__NODE_LOCKER__H__


#include <shared_mutex>
#include <unordered_map>
#include <list>
#include <atomic>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PathLocker
    {
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;
        std::shared_mutex guard_;
        std::unordered_map< NodeUid, std::atomic< size_t > > locked_nodes_;
        
    public:

        class PathLock;

        PathLocker( ) : locked_nodes_( Policies::PhysicalVolumePolicy::MountPointLimit ) {}

        PathLock lock_node( NodeUid node )
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

        bool is_removable( NodeUid node )
        {
            std::shared_lock l{ guard_ };
            return locked_nodes_.count( node ) == 0;
        }
    };


    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PathLocker::PathLock
    {
        std::list< std::function< void( ) > > unlocks_;

        friend class PathLocker;

        PathLock( std::function< void( ) > && unlock ) noexcept
        {
            assert( unlock );
            unlocks_.push_back( unlock );
        }

        void unlock_all( )
        {
            std::for_each( rbegin( unlocks_ ), rend( unlocks_ ), [=] ( auto & unlock ) {
                assert( unlock );
                unlock( );
            } );

            unlocks_.clear( );
        }

    public:

        PathLock( ) = default;
        PathLock( const PathLock & ) = delete;
        PathLock& operator = ( const PathLock & ) = delete;

        PathLock( PathLock&& o )
        {
            unlock_all( );
            unlocks_ = move( o.unlocks_ );
        }

        PathLock& operator = ( PathLock&& o )
        {
            unlock_all( );
            unlocks_ = move( o.unlocks_ );
            return *this;
        }

        PathLock & operator << ( PathLock && other )
        {
            unlocks_.splice( end( unlocks_ ), other.unlocks_ );
            return *this;
        }

        ~PathLock( )
        {
            unlock_all( );
        }
    };
}

#endif