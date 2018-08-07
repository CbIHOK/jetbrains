#ifndef __JB__VARIADIC_HASH__H__
#define __JB__VARIADIC_HASH__H__

#include <type_traits>
#include <functional>

namespace jb
{
    namespace misc
    {
        template <typename T>
        struct hash_fn_selector
        {
            static constexpr bool has_std_hash = std::is_same< size_t, decltype(std::hash< T >()(*(T*)0)) >::value;
            //
            typedef char no[1];
            typedef char yes[2];
            typedef char stdh[3];

            template <typename U, U u> struct reallyHas;
            template <typename C> static yes& test(reallyHas<size_t (C::*)(), &C::hash>*) {}
            template <typename C> static yes& test(reallyHas<size_t (C::*)() const, &C::hash>*) {}
            template <typename C> static stdh& test(decltype(std::hash<C>()(C()))) {}
            template <typename> static no& test(...) {}

            using ht = decltype(test<T>(0));

            static constexpr bool has_own_hash = sizeof(test<T>(0)) == sizeof(yes);
        };


        template < typename T >
        size_t variadic_hash( const T & v ) noexcept
        {
            bool h = hash_fn_selector< T >::has_own_hash;
            size_t sz = sizeof(hash_fn_selector< T>::ht);

            if (hash_fn_selector<T>::has_own_hash)
            {
                //return v.hash();
            }
            else if (hash_fn_selector<T>::has_std_hash)
            {
                return std::hash< T >()(v);
            }
            else
            {
                static_assert(hash_fn_selector<T>::has_own_hash || hash_fn_selector<T>::has_std_hash, "No hashing operation defined for T");
                return  0;
            }
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