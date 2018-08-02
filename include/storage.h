#include "virtual_volume.h"
#include <set>
#include <memory>
#include <mutex>

namespace jb
{
    template < typename T >
    class Storage
    {
        static auto & virtual_volumes()
        {
            static std::mutex guard;
            static std::set< std::shared_ptr< VirtualVolumeImpl > > holder;
            return std::make_tuple(guard, holder);
        }

    public:

    };
}