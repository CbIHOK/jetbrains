#include <mutex>
#include <vector>
#include <assert.h>
#include <storage.h>
#include <policies.h>
#include <iostream>
#include <btree.h>
#include <key.h>
#include <filesystem>


int main()
{
    using Storage = jb::Storage< jb::DefaultPolicies >;
    using Key = jb::details::Key< jb::DefaultPolicies >;

    Key k{ "////foo/boo////goo//1" };
    bool v = k;
    auto[key, subkey] = k.break_apart();

    return 0;
}
