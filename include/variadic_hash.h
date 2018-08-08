#ifndef __JB__VARIADIC_HASH__H__
#define __JB__VARIADIC_HASH__H__

#include <functional>
#include <type_traits>

namespace jb
{
    namespace misc
    {
        template < typename T >
        struct detect_std_hash
        {
            using HashT = std::hash< T >;

            template < typename U > 
            static constexpr decltype( std::declval< std::hash< U > >()( U() ), bool() ) test( T* )
            {
                return true;
            }

            template < typename > static constexpr bool test( ... ) { return false; }

            static constexpr bool value = test< T >( nullptr );
        };



        template < typename Policies, typename Pad, typename T >
        size_t variadic_hash( const T & v ) noexcept
        {
            if constexpr ( Hash< Policies, Pad, T >::enabled )
            {
                return Hash< Policies, Pad, T >()( v );
            }
            else if constexpr ( detect_std_hash< T >::value )
            {
                return std::hash< T >()( v );
            }
            else
            {
                static_assert( false, "No hashing operation defined" );
            }
        }

        template < typename Policies, typename Pad, typename T, typename... Args >
        size_t variadic_hash(const T & v, const Args &... args) noexcept
        {
            size_t seed = variadic_hash< Policies, Pad >(args...);
            return std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
    }
}

#endif