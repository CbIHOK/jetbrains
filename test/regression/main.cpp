#include <mutex>
#include <vector>
#include <assert.h>
#include <storage.h>
#include <policies.h>
#include <iostream>


using Storage = jb::Storage< policies::DefaultPolicies >;


auto foo()
{
    static std::mutex guard;
    static std::vector< int > data;
    return std::forward_as_tuple(guard, data);
}


int main()
{
    auto[open_ret, handle] = Storage::OpenVirtualVolume();

    std::cout << typeid(handle).name() << std::endl;

    auto[ret1] = Storage::CloseAll();
    auto[ret2] = Storage::Close(handle);
    
    return 0;
}
