#include "test.h"
#include <policies.h>


using namespace std;


struct BTreePower_32 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1 << 20;
        static constexpr size_t BTreeMinPower = 32;
        static constexpr size_t BTreeCacheSize = 32;
    };
};


void b_tree_power_32_test()
{
    performance_test< BTreePower_32 >();
}
