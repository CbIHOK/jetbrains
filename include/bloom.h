#ifndef __JB__BLOOM__H__
#define __JB__BLOOM__H__


#include <SHAtwo/SHAtwo.h>
#include <shared_mutex>
#include <bitset>
#include <array>


namespace jb
{
    template < typename Policies >
    class Bloom
    {
        using KeyCharT = typename Policies::KeyCharT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;


        static constexpr size_t BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr size_t BloomFnCount = Policies::PhysicalVolumePolicy::BloomFnCount;


        static bool constexpr power_of_2( size_t value )
        {
            return value && ( value & ( value - 1 ) ) == 0;
        }


        static_assert( power_of_2( BloomSize ), "Should be power of 2" );
        static_assert( BloomFnCount <= 16, "Maximum suppoerted number of Bloom functions is 16" );


        mutable std::shared_mutex guard_;


        static constexpr size_t bits_in_byte = 8;
        static constexpr size_t filter_size = bits_in_byte * Policies::PhysicalVolumePolicy::BloomSize;
        std::bitset< filter_size > filter_;


        static auto get_digest( KeyRefT key ) noexcept
        {
            static constexpr size_t digets_placeholder_size = 20;
            std::array< uint32_t, digets_placeholder_size > digest;

            SHAtwo sha512{};

            sha512.HashData( 
                reinterpret_cast< uint8_t * >( const_cast< char *>( key.data() ) ), 
                static_cast< uint32_t >( key.length() * sizeof( KeyRefT::value_type ) ) );

            sha512.GetDigest( reinterpret_cast< uint8_t *>( const_cast< uint32_t *>( digest.data() ) ) );

            return digest;
        }


    public:

        Bloom() = default;
        virtual ~Bloom() noexcept = default;

        Bloom( const Bloom & ) = delete;
        Bloom & operator = ( const Bloom & ) = delete;

        Bloom( Bloom && ) noexcept = default;
        Bloom & operator = ( Bloom && ) noexcept = default;


        auto add( KeyRefT key ) noexcept
        {
            using namespace std;

            auto digest{ move( get_digest( key ) ) };

            unique_lock< shared_mutex > lock( guard_ );

            for ( size_t i = 0; i < BloomFnCount; ++i )
            {
                filter_.set( ( digest[ i ] % filter_size ) );
            }
        }


        auto test( KeyRefT key ) const noexcept
        {
            using namespace std;

            auto digest{ move( get_digest( key ) ) };

            shared_lock< shared_mutex > lock( guard_ );

            for ( size_t i = 0; i < BloomFnCount; ++i )
            {
                if ( ! filter_.test( ( digest[ i ] % filter_size ) ) )
                {
                    return false;
                }
            }

            return true;
        }
    };
}

#endif