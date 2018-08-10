#ifndef __JB__BLOOM__H__
#define __JB__BLOOM__H__

#include <SHAtwo/SHAtwo.h>
#include <shared_mutex>


class TestBloom;


namespace jb
{
    template < typename Policies, typename Pad >
    class Bloom
    {
        class TestBloom;

        using KeyRefT = typename Policies::KeyRefT

        std::shared_mutex guard_;
        std::bitset< 8 * ( 1 << 20 ) > filter_;

    public:

        Bloom();

        auto add( KeyRefT key );
        auto test( KeyRefT key );
    };
}

#endif