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
        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );

        using NodeUid = unsigned long long;

        static constexpr auto RootNodeUid = 0;
        static constexpr auto InvalidNodeUid = std::numeric_limits< NodeUid >::max( );
        static constexpr auto Npos = std::numeric_limits< size_t >::max();

        static constexpr auto BTreePower = Policies::PhysicalVolumePolicy::BTreePower;
        static_assert( BTreePower >= 2, "B-tree power must be >1" );
        static_assert( BTreePower + 1 < std::numeric_limits< size_t >::max( ), "B-tree power is too great" );

        NodeUid uid_;
        boost::container::static_vector< KeyHashT, BTreePower> hashes_;
        boost::container::static_vector< Value, BTreePower > values_;
        boost::container::static_vector< Timestamp, BTreePower > expirations_;
        boost::container::static_vector< NodeUid, BTreePower > children_;
        boost::container::static_vector< uint8_t, BTreePower > erased_marks_;
        boost::container::static_vector< NodeUid, BTreePower + 1 > links_;
        bool changed = false;
    };


    template < typename Policies, typename Pad >
    class BTree
    {
    public:

        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );
        using NodeUid = b_tree_node::NodeUid;


        BTree( NodeUid entry_uid )
        {

        }

        auto find( const Key & relative ) {}

    };

}


#endif