namespace jb
{
    namespace detail
    {
        constexpr size_t hash_constant() noexcept
        {
            static_assert( sizeof( size_t ) == sizeof( uint32_t ) || sizeof( size_t ) == sizeof( uint64_t ), "Unable to identify 32-bit or 64-bit platform" );
            return sizeof( size_t ) == sizeof( uint64_t ) ? 0x9E3779B97F4A7C15ULL : 0x9e3779b9U;
        }

        template < typename T >
        constexpr size_t combine_hash( const T & value, size_t seed ) noexcept
        {
            static constexpr std::hash< T > h{};
            return h( value ) + hash_constant() + ( seed << 6 ) + ( seed >> 2 );
        }

        template < typename T, typename... Args >
        size_t variadic_hash( const T & value, const Args &... args ) noexcept
        {
            const auto seed = combine_hash( args... );
            return hash( value, seed );
        }

        template < typename T >
        static auto variadic_hash( const T & value ) noexcept
        {
            static constexpr shd::hash< T > h{};
            return h( value );
        }
    }
}