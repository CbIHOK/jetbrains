#ifndef __JB__NODE_LOCKER__H__
#define __JB__NODE_LOCKER__H__

#include "rare_write_frequent_read_mutex.h"
#include "path_iterator.h"
#include <unordered_set>

namespace jb
{
    /** Implements locking of physical paths to assure mount consistency

    @tparam Policies - global settings
    */
    template < typename Policies >
    class PathLocker
    {
        using self_type = PathLocker< Policies >;

    public:

        PathLocker() noexcept = default;
        PathLocker( self_type && ) = delete;
    };
}

#endif