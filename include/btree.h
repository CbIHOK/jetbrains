#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include "arithmetic.h"
#include <utility>
#include <boost/container/static_vector.hpp>


template < typename Policies >
class BTreeNode
{
    using ValueT   = typename Policies::ValueT;
    using KeyRefT  = typename Policies::KeyPolicy::KeyRefT;
    using KeyHashF = typename Policies::KeyPolicy::KeyHashF;
    using KeyHashT = typename Policies::KeyPolicy::KeyHashT;
    using LinkT    = typename Policies::StoragePolicy::IndexT;

    static constexpr size_t Power = Policies::BTreePolicy::Power;
    static_assert(arithmetic::is_power_of_2(Power), "Using B-Tree Power other then 2^N may decrease memory/performance efficency");

    boost::container::static_vector< KeyHashT, Power >  key_hashes_;
    boost::container::static_vector< ValueT, Power >    values_;
    boost::container::static_vector< LinkT, Power + 1 > children_;
};


#endif