#ifndef __JB__PHYSICAL_STORAGE__H__
#define __JB__PHYSICAL_STORAGE__H__


#include <limits>
#include <memory>
#include <unordered_map>
#include <list>
#include <filesystem>
#include <exception>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>


namespace jb
{
    /** Implements volume <--> file data exchange

    The main purpose for the class is to assure correct sharing of B tree nodes (that is actually
    how the data represented on physical level). The class guaranties that each B tree node from
    a physical file has the only reflection on logical level.

    @tparam Policies - global setting
    @tparam Pad - test pad
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PhysicalStorage
    {

    public:

        using RetCode = typename Storage::RetCode;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;

        typedef uint64_t NodeUid;
        static constexpr NodeUid RootNodeUid = 0;
        static constexpr NodeUid InvalidNodeUid = std::numeric_limits< uint64_t >::max( );

        class BTree;
        using BTreeP = std::shared_ptr< BTree >;

    private:

        RetCode creation_status_ = RetCode::Ok;

        using MruOrder = std::list< NodeUid >;
        using MruItems = std::unordered_map< NodeUid, std::pair< BTreeP, MruOrder::const_iterator > >;

        boost::upgrade_mutex mru_mutex_;
        MruOrder mru_order_;
        MruItems mru_items_;
        static constexpr auto CacheSize = Policies::PhysicalVolumePolicy::BTreeCacheSize;


    public:

        /** The class is not default constructible
        */
        PhysicalStorage( ) = delete;


        /** The class is not copyable/movable
        */
        PhysicalStorage( PhysicalStorage&& ) = delete;


        /** Constructs physical storage

        @param [in] path - file name
        @throw nothing
        @note check object validity by creation_status()
        */
        explicit PhysicalStorage( const std::filesystem::path && file ) try
            : mru_order_( CacheSize, InvalidNodeUid )
            , mru_items_( CacheSize )
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


        /** Provides creation status

        @retval RetCode - contains Ok if object was successfully created and an error code otherwise
        @throw nothing
        */
        RetCode creation_status( ) const noexcept { return creation_status_; }


        /** Provides requested B-tree node from MRU cache

        @param [in] uid - node UID
        @retval RetCode - operation status
        @retval BTreeP - if operation succeeds holds shared pointer to requested item
        @throw nothing
        */
        std::tuple< RetCode, BTreeP > get_node( NodeUid uid ) noexcept
        {
            using namespace boost;

            try
            {
                // shared lock over the cache
                upgrade_lock< upgrade_mutex > shared_lock{ mru_mutex_ };

                if ( auto item_it = mru_items_.find( uid ); item_it != mru_items_.end( ) )
                {
                    // get exclusive lock over the cache
                    upgrade_to_unique_lock< upgrade_mutex > exclusive_lock{ shared_lock };

                    // mark the item as MRU
                    mru_order_.splice( end( mru_order_ ), mru_order_, item_it->second.second );

                    return { RetCode::Ok, item_it->second.first };
                }
                else
                {
                    // through the order list
                    for ( auto order_it = begin( mru_order_ ); order_it != end( mru_order_ ); ++order_it )
                    {
                        // is free?
                        if ( InvalidNodeUid == *order_it )
                        {
                            break;
                        }

                        // if item is not used anymore
                        if ( auto item_it = mru_items_.find( *order_it ); item_it->second.first.use_count( ) == 1 )
                        {
                            // get exclusive lock over the cache
                            upgrade_to_unique_lock< upgrade_mutex > exclusive_lock{ shared_lock };

                            // drop the item from cache
                            mru_items_.erase( item_it );
                            *order_it = InvalidNodeUid;

                            // move order record to the end of the list
                            mru_order_.splice( end( mru_order_ ), mru_order_, order_it );

                            break;
                        }
                    }

                    // if cache is still full that means that there are too many concurrent operations on the physical volume
                    if ( mru_order_.back( ) != InvalidNodeUid )
                    {
                        return { RetCode::TooManyConcurrentOps, BTreeP{} };
                    }

                    if ( uid == RootNodeUid )
                    {
                        // get exclusive lock over the cache
                        upgrade_to_unique_lock< upgrade_mutex > exclusive_lock{ shared_lock };

                        auto root = std::make_shared< BTree >();
                        mru_order_.front() = RootNodeUid;
                        mru_items_.emplace( RootNodeUid, std::move( std::pair{ root, mru_order_.begin() } ) );

                        return { RetCode::Ok, root };
                    }
                }
            }
            catch ( ... )
            {

            }

            return { RetCode::UnknownError, BTreeP{} };
        }
    };
}


#include "b_tree.h"


#endif
