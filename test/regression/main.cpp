#include <vector>

static std::vector< int> m = { 0, 1, 2, 3, 4, 5 };

auto foo()
{
    return std::make_tuple(true, m[2]);
}

int main()
{
    auto[status, result] = foo();
    return 0;
}
