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
        using KeyHashT = typename Hash< Policies, Pad, Key >::type;

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

            assert( prefix.is_path() );
            assert( suffix.is_path() );

            array< KeyHashT, BloomPrecision > chunks;
            fill( begin( chunks ), end( chunks ), KeyHashT{} );
            auto chunk_it = begin( chunks );

            auto get_chunk_hahses = [&] ( const auto & v ) {
                auto key = v;

                while ( key.size() && chunk_it != end( chunks ) )
                {
                    auto[ split_ok, chunk, rest ] = key.split_at_head();
                    assert( split_ok );
                    key = rest;

                    auto[ stem_ok, stem ] = chunk.cut_lead_separator();
                    assert( stem_ok );

                    static constexpr Hash< Policies, Pad, Key > hasher{};
                    *chunk_it = hasher( stem );
                    ++chunk_it;
                };
            };

            if ( Key::root() != prefix ) get_chunk_hahses( prefix );
            if ( Key::root() != suffix )get_chunk_hahses( suffix );

            // calculate SHA-512
            SHAtwo sha512{};
            sha512.HashData( reinterpret_cast< uint8_t * >( chunks.data( ) ), static_cast< uint32_t >( chunks.size( ) * sizeof( KeyHashT ) ) );

            // extract SHA-512 digest
            static constexpr size_t digets_placeholder_size = 20;
            array< uint32_t, digets_placeholder_size > digest;
            sha512.GetDigest( reinterpret_cast< uint8_t *>( const_cast< uint32_t *>( digest.data() ) ) );

            return digest;
        }


    public:

        using Digest = std::array< uint32_t, 20 >;

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
