#include "test.h"
#include <policies.h>


using namespace std;


struct BTreePower_16 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1 << 20;
        static constexpr size_t BTreeMinPower = 16;
        static constexpr size_t BTreeCacheSize = 128;
    };
};


void b_tree_power_16_test()
{
    performance_test< BTreePower_16 >();
}
