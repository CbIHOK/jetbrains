#ifndef __JB__VARIADIC_HASH__H__
#define __JB__VARIADIC_HASH__H__

#include <functional>

namespace jb
{
    namespace misc
    {
        template < typename T >
        size_t variadic_hash( const T & v ) noexcept
        {
            return std::hash<T>()(v);
        }

        template < typename T, typename... Args >
        size_t variadic_hash(const T & v, const Args &... args) noexcept
        {
            size_t seed = variadic_hash(args...);
            return std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
    }
}

#endif