#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <tuple>
#include <algorithm>
#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_types.hpp>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PhysicalStorage::BTree
    {
        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );

        using NodeUid = PhysicalStorage::NodeUid;
        static constexpr auto RootNodeUid = PhysicalStorage::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalStorage::InvalidNodeUid;

        static constexpr auto BTreePower = Policies::PhysicalVolumePolicy::BTreePower;
        static_assert( BTreePower >= 2, "B-tree power must be >1" );
        static_assert( BTreePower + 1 < std::numeric_limits< ptrdiff_t >::max( ), "B-tree power is too great" );

        PhysicalStorage * storage_;
        NodeUid parent_;
        mutable boost::shared_mutex guard_;
        boost::container::static_vector< KeyHashT, BTreePower> hashes_;
        boost::container::static_vector< Value, BTreePower > values_;
        boost::container::static_vector< Timestamp, BTreePower > expirations_;
        boost::container::static_vector< NodeUid, BTreePower > children_;
        boost::container::static_vector< bool, BTreePower > erased_marks_;
        boost::container::static_vector< NodeUid, BTreePower + 1 > links_;
        bool changed = false;

    public:

        /** The class is not copyable/movable
        */
        BTree( BTree&& ) = delete;


        BTree( )
        {
            links_.push_back( InvalidNodeUid );
        }

        boost::shared_lock< boost::shared_mutex > get_shared_lock( )
        {
            return boost::shared_lock< boost::shared_mutex >{ guard_ };
        }

        Value value( size_t ndx ) const
        { 
            assert( ndx < values_.size( ) );
            return move( values_[ ndx ] );
        }

        RetCode set_value( size_t ndx, Value&& value, boost::shared_lock<boost::shared_mutex> && read_lock )
        {
            assert( ndx < values_.size( ) );

            boost::upgrade_lock upgrade_lock{ guard_ };
            {
                auto consume_read_lock = move( read_lock );
            }

            boost::upgrade_to_unique_lock write_lock{ upgrade_lock };
            values_[ ndx ] = move( value );
        }

        Timestamp expiration( size_t ndx ) const
        {
            assert( ndx < expirations_.size( ) );
            return move( expirations_[ ndx ] );
        }

        NodeUid child( size_t ndx ) const
        {
            assert( ndx < children_.size( ) );
            return move( children_[ ndx ] );
        }

        bool erased( size_t ndx ) const
        {
            assert( ndx < erased_marks_.size( ) );
            return erased_marks_[ ndx ];
        }

        void set_erased( size_t ndx ) const
        {
            assert( ndx < values_.size( ) );

            boost::upgrade_lock upgrade_lock{ guard_ };
            {
                auto consume_read_lock = move( read_lock );
            }

            boost::upgrade_to_unique_lock write_lock{ upgrade_lock };
            erased_marks_[ ndx ] = true;
        }

        /** Checks if key presents in the node

        If key presents returns 
        */
        std::tuple< size_t, NodeUid > find_key( const Key & key ) const
        {
            using namespace std;

            assert( key.is_leaf() );
            
            assert( hashes_.size() == values_.size() );
            assert( hashes_.size() == expirations_.size() );
            assert( hashes_.size() == children_.size() );
            assert( hashes_.size() == erased_marks_.size() );
            assert( hashes_.size() + 1 == links_.size() );

            static constexpr Hash< Policies, Pad, Key > hasher;
            auto hash = hasher( key );

            if ( auto lower = lower_bound( begin( hashes_ ), end( hashes_ ) ); lower == hashes_.end() )
            {
                return tuple{ Npos, links_.back() };
            }
            else
            {
                assert( 0 <= distance( begin( hashes_ ), lower ) && distance < hashes_.size() );
                auto d = static_cast< size_t >( distance( begin( hashes_ ), lower ) );
                
                if ( *lower == hash )
                {
                    return tuple{ d, InvalidNodeUid };
                }
                else
                {
                    return tuple{ Npos, links_[ d ] };
                }
            }
        }
    };
}


#endif