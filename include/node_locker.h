#include <atomic>
#include <thread>
#include <memory>
#include <new>
#include <mutex>
#include <unordered_map>
#include <boost/shared_mutex.hpp>


namespace jb
{
    namespace detail
    {
        class trivial_node_locker
        {
        };

        class lock_free_node_locker
        {
            
        };
    }
}
