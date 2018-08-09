#ifndef __JB__VARIADIC_HASH__H__
#define __JB__VARIADIC_HASH__H__


#include <functional>
#include <utility>


namespace jb
{
    namespace misc
    {
        //
        // Checks if std::hash<> specialized for type T using SFINAE
        //
        template < typename T >
        struct detect_std_hash
        {
            template < typename U > 
            static constexpr decltype( std::declval< std::hash< U > >()( U() ), bool() ) test( T* )
            {
                return true;
            }

            template < typename >
            static constexpr bool test( ... ) { return false; }

            //
            // Keeps true if std:hash<> is specialized for T, otherwise keeps false 
            //
            static constexpr bool value = test< T >( nullptr );
        };


        //
        // Provides hashing operations for types. Uses jb::Hash<> if it has specialization for T,
        // otherwise use std::hash<>. If neither jb::Hash<> nor std::hash<> are specialized for T,
        // generates compilation error.
        //
        template < typename Policies, typename Pad >
        struct hash_selector
        {
            template< typename T >
            size_t operator () ( const T & v ) const noexcept
            {
                static_assert(
                    Hash< Policies, Pad, T >::enabled || detect_std_hash< T >::value,
                    "No hashing operation defined for the type"
                    );

                if constexpr ( Hash< Policies, Pad, T >::enabled )
                {
                    static constexpr Hash< Policies, Pad, T > hasher{};
                    return hasher( v );
                }
                else
                {
                    static constexpr std::hash< T > hasher{};
                    return hasher( v );
                }
            }
        };


        //
        // Provides hash combining constant depending on size of size_t type
        //
        static constexpr size_t hash_constant( ) noexcept
        {
            static_assert(
                sizeof( size_t ) == 8 || sizeof( size_t ) == 4,
                "Cannot detect 32-bit or 64-bit platform"
                );

            if constexpr ( sizeof( size_t ) == 8 )
            {
                return 0x9E3779B97F4A7C15ULL;
            }
            else
            {
                return 0x9e3779b9U;
            }
        }


        //
        // Provides comined hash value for a variadic sequence of agruments
        //
        template < typename Policies, typename Pad, typename T, typename... Args >
        size_t variadic_hash(const T & v, const Args &... args) noexcept
        {
            size_t seed = variadic_hash< Policies, Pad >(args...);
            return hash_selector< Policies, Pad >{}( v ) + hash_constant() + ( seed << 6 ) + ( seed >> 2 );
        }


        //
        // Just a terminal specialization of variadic template
        //
        template < typename Policies, typename Pad, typename T >
        size_t variadic_hash( const T & v ) noexcept
        {
            return hash_selector< Policies, Pad >{}( v );
        }
    }
}

#endif