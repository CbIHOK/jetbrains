#ifndef __JB__PHYSICAL_STORAGE__H__
#define __JB__PHYSICAL_STORAGE__H__


#include <limits>
#include <memory>
#include <unordered_map>
#include <list>
#include <filesystem>
#include <exception>

#include <boost/thread/shared_mutex.hpp>


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

        boost::shared_mutex mru_mutex_;
        MruOrder mru_order_;
        MruItems mru_items_;
        static constexpr auto CacheSize = Policies::PhysicalVolumePolicy::BTreeCacheSize;


    public:

        PhysicalStorage( ) = delete;
        PhysicalStorage( PhysicalStorage&& ) = delete;

        PhysicalStorage( const std::filesystem::path & file ) noexcept try
            : mru_order_{ CacheSize, InvalidNodeUid }
            , mru_items_{ CacheSize }
        {

        }
        catch ( const std::bad_alloc & )
        {
            creation_status_ = RetCode::InsuficcientMemory;
        }
        catch ( ... )
        {
            creation_status_ = RetCode::UnknownError;
        }


        /** Provides creation status
        */
        RetCode creation_status( ) const noexcept { return creation_status_; }


        /** Provides requested B tree node

        @param [in] uid - node UID
        @retval RetCode - operation status
        @retval BTreeP - if operation succeeds holds shared pointer to requested item
        @throw 
        */
        std::tuple< RetCode, BTreeP > get_node( NodeUid uid ) noexcept
        {
            using namespace std;

            try
            {
                // shared lock over the cache
                boost::shared_lock read_lock{ mru_mutex_ };

                if ( auto item_it = mru_items_.find( uid ); item_it != mru_items_.end( ) )
                {
                    // get exclusive lock over the cache
                    boost::upgrade_lock upgrade_lock{ mru_mutex_ };
                    boost::upgrade_to_unique_lock write_lock{ upgrade_lock };

                    // mark the item as MRU
                    mru_order_.splice( end( mru_order_ ), mru_order_, item_it->second.second );

                    return { RetCode::Ok, item_it->second.first };
                }
                else
                {
                    // through the order list
                    for ( auto order_it = begin( mru_order_ ); order_it != end( mru_order_ ); ++order_it )
                    {
                        // if item is not used anymore
                        if ( auto item_it = mru_items_.find( *order_it ); item_it->second.first.use_count( ) == 1 )
                        {
                            // get exclusive lock over the cache
                            boost::upgrade_lock upgrade_lock{ mru_mutex_ };
                            boost::upgrade_to_unique_lock write_lock{ upgrade_lock };

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
