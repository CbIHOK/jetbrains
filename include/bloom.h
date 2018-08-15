#ifndef __JB__BLOOM__H__
#define __JB__BLOOM__H__

#include <shared_mutex>
#include <bitset>
#include <array>
#include <execution>
#include <boost/container/static_vector.hpp>
#include <storage.h>
#include <SHAtwo/SHAtwo.h>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::Bloom
    {
        using Key = typename PhysicalVolumeImpl::Key;
        using KeyCharT = typename Key::CharT;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto BloomFnCount = Policies::PhysicalVolumePolicy::BloomFnCount;
        static constexpr auto BloomPrecision = Policies::PhysicalVolumePolicy::BloomPrecision;

        static bool constexpr power_of_2( size_t value )
        {
            return value && ( value & ( value - 1 ) ) == 0;
        }

        static_assert( power_of_2( BloomSize ), "Should be power of 2" );
        static_assert( BloomFnCount <= 16, "Number of Bloom functions Must be in range [1,16]" );

        static constexpr size_t bits_in_byte = 8;
        static constexpr size_t filter_size = bits_in_byte * Policies::PhysicalVolumePolicy::BloomSize;


        mutable std::shared_mutex guard_;
        std::bitset< filter_size > filter_;


        /* Provides Bloom functions for combination of given keys

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        @return std::array< uint32_t, 20 > holding Bloom functions for combined key
        */
        static auto get_digest( const Key & prefix, const Key & suffix )
        {
            using namespace std;
            using namespace boost::container;

            // merge keys on stack
            array< KeyCharT, BloomPrecision > combined;

            auto sz_1 = std::min( prefix.size( ), BloomPrecision );
            copy_n( execution::par, begin( prefix ), sz_1, begin( combined ) );

            auto sz_2 = std::min( suffix.size(), BloomPrecision - sz_1);
            copy_n( execution::par, begin( suffix ), sz_2, begin( combined ) + sz_1 );

            // calculate SHA-512
            SHAtwo sha512{};
            sha512.HashData( reinterpret_cast< uint8_t * >( combined.data( ) ), static_cast< uint32_t >(combined.size( ) * sizeof( KeyCharT ) ) );

            // extract SHA-512 digest
            static constexpr size_t digets_placeholder_size = 20;
            array< uint32_t, digets_placeholder_size > digest;
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


        /** Updates the filter adding another combinations of keys

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        */
        auto add( const Key & prefix, const Key & suffix )
        {
            using namespace std;

            auto digest{ move( get_digest( prefix, suffix ) ) };

            unique_lock< shared_mutex > lock( guard_ );

            for ( size_t i = 0; i < BloomFnCount; ++i )
            {
                filter_.set( ( digest[ i ] % filter_size ) );
            }
        }


        /** Checks if combination of given keys may be in a set

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        @return false if the combination is not in set, true means that the combination MAY BE in a set
        */
        auto test( const Key & prefix, const Key & suffix ) const
        {
            using namespace std;

            auto digest{ move( get_digest( prefix, suffix ) ) };

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
