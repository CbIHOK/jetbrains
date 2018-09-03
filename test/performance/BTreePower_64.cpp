#include "test.h"
#include <policies.h>


using namespace std;


struct BTreePower_64 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1 << 20;
        static constexpr size_t BTreeMinPower = 64;
        static constexpr size_t BTreeCacheSize = 32;
    };
};


void b_tree_power_64_test()
{
    performance_test< BTreePower_64 >();
}
