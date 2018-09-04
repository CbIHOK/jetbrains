#include <policies.h>
#include "test.h"


using namespace std;


extern void b_tree_power_16_test();
extern void b_tree_power_32_test();
extern void b_tree_power_64_test();
extern void b_tree_power_128_test();


int main( int argc, char **argv )
{
    b_tree_power_16_test();
    b_tree_power_32_test(); // MUST be optimal for 25000 node, cuz 32^3 = 32768
    b_tree_power_64_test();
    b_tree_power_128_test();
    performance_test< jb::DefaultPolicy<> >();
    return 0;
}
