#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include "arithmetic.h"
#include <utility>
#include <boost/container/static_vector.hpp>


template < typename Policies >
class BTreeNode
{
    template< typename T > friend class BTreeTest;

    using ValueT      = typename Policies::ValueT;
    using KeyRefT     = typename Policies::KeyPolicy::KeyRefT;
    using KeyHashF    = typename Policies::KeyPolicy::KeyHashF;
    using KeyHashT    = typename Policies::KeyPolicy::KeyHashT;
    using KeyHashRefT = typename Policies::KeyPolicy::KeyHashRefT;
    using LinkT       = typename Policies::StoragePolicy::IndexT;

    static constexpr auto Power = Policies::BTreePolicy::Power;
    static_assert(arithmetic::is_power_of_2(Power), "Using B-Tree Power other then 2^N may decrease memory/performance efficency");

    boost::container::static_vector< KeyHashT, Power >  key_hashes_;
    boost::container::static_vector< ValueT, Power >    values_;
    boost::container::static_vector< LinkT, Power + 1 > children_;

public:

    BTreeNode() noexcept = default;

    /**
    */
    auto Get(KeyHashRefT key_hash)
    {
        assert(!key_hashes_.empty());

        auto ge_it = std::lower_bound(key_hashes_.begin(), key_hashes_.end(), key_hash);

        auto index = std::distance(key_hashes_.begin(), ge_it);
        assert(index);

        if (key_hash == *ge_it)
        {
            return std::make_tuple(true, values_[index]);
        }
        else
        {
            BTreeNode child;
            return child.Get(key_hash);
        }
    }

};


#endif