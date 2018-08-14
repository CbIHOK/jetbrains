#ifndef __JB__NODE_LOCKER__H__
#define __JB__NODE_LOCKER__H__


#include <shared_mutex>
#include <unordered_map>
#include <atomic>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::NodeLocker
    {
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;
        std::shared_mutex guard_;
        std::unordered_map< NodeUid, std::atomic< size_t > > locked_nodes_;
        
    public:

        class NodeLock;

        NodeLocker( ) : locked_nodes_( Policies::PhysicalVolumePolicy::MountPointLimit ) {}

        NodeLock lock_node( NodeUid node )
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

            return NodeLock( unlock );
        }

        bool is_removable( NodeUid node )
        {
            std::shared_lock l{ guard_ };
            return locked_nodes_.count( node ) == 0;
        }
    };


    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::NodeLocker::NodeLock
    {
        std::function< void( ) > unlock_;

        friend class NodeLocker;

        NodeLock( std::function< void( ) > && unlock ) noexcept : unlock_( unlock )
        {
            assert( unlock_ );
        }

    public:

        NodeLock( ) noexcept = delete;
        NodeLock( const NodeLock & ) = delete;
        NodeLock& operator = ( const NodeLock & ) = delete;

        NodeLock( NodeLock&& o )
        {
            if ( unlock_ ) unlock_( );
            unlock_ = move( o.unlock_ );
        }

        NodeLock& operator = ( NodeLock&& o )
        {
            if ( unlock_ ) unlock_( );
            unlock_ = move( o.unlock_ );
            return *this;
        }

        ~NodeLock( )
        {
            if ( unlock_ ) unlock_( );
        }
    };
}

#endif