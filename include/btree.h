#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <limits>
#include <bitset>
#include <tuple>
#include <algorithm>
#include <boost/container/static_vector.hpp>
#include <storage.h>


namespace jb
{
    template < typename Policies, typename Pad >
    struct b_tree_node
    {
        b_tree_node( )
        {
            links_.push_back( InvalidNodeUid );
        }

        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );

        using BTreeNodeUid = unsigned long long;

        static constexpr auto RootNodeUid = 0;
        static constexpr auto InvalidNodeUid = std::numeric_limits< BTreeNodeUid >::max( );
        static constexpr auto Npos = std::numeric_limits< size_t >::max();

        static constexpr auto BTreePower = Policies::PhysicalVolumePolicy::BTreePower;
        static_assert( BTreePower >= 2, "B-tree power must be >1" );
        static_assert( BTreePower + 1 < std::numeric_limits< size_t >::max( ), "B-tree power is too great" );

        BTreeNodeUid uid_;
        boost::container::static_vector< KeyHashT, BTreePower> hashes_;
        boost::container::static_vector< Value, BTreePower > values_;
        boost::container::static_vector< Timestamp, BTreePower > expirations_;
        boost::container::static_vector< BTreeNodeUid, BTreePower > children_;
        boost::container::static_vector< uint8_t, BTreePower > erased_marks_;
        boost::container::static_vector< BTreeNodeUid, BTreePower + 1 > links_;
        bool changed = false;

        auto find( const Key & key )
        {
            using namespace std;

            assert( key.is_leaf( ) );
            
            static constexpr Hash< Policies, Pad, Key > hasher;
            auto hash = hasher( key );

            assert( hashes_.size( ) == values_.size( ) );
            assert( hashes_.size( ) == expirations_.size( ) );
            assert( hashes_.size( ) == children_.size( ) );
            assert( hashes_.size( ) == erased_marks_.size( ) );
            assert( hashes_.size( ) + 1 == links_.size( ) );
            
            if ( auto lower = lower_bound( begin( hashes_ ), end( hashes_ ), hash ); lower == hashes_.end( ) )
            {
                return tuple{ Npos , links_.back( ) };
            }
            else if ( *lower != hash )
            {
                assert( distance( hashes_, lower ) < links_.size( ) );
                return tuple{ Npos , links_[ distance( hashes_, lower ) ] };
            }
            else
            {
                return tuple{ distance( hashes_, lower ), InvalidNodeUid };
            }
        }
    };
}


#endif