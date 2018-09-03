#include "test.h"
#include <policies.h>


using namespace std;


struct BTreePower_128 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1 << 20;
        static constexpr size_t BTreeMinPower = 128;
        static constexpr size_t BTreeCacheSize = 32;
    };
};


void b_tree_power_128_test()
{
    performance_test< BTreePower_128 >();
}
