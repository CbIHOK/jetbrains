#include <mutex>
#include <vector>
#include <assert.h>
#include <storage.h>
#include <policies.h>
#include <iostream>
#include <btree.h>
#include <filesystem>


int main()
{
    using namespace jb;

    using Storage = Storage< DefaultPolicies >;

    auto[r, volume ]= Storage::OpenVirtualVolume();
    auto[r1, m] = volume.Mount( Storage::PhysicalVolume(), "/foo/boo/", "/111/222" );
    volume.Close();

    return 0;
}
