#include <gtest/gtest.h>
#include <storage.h>
#include <unordered_set>
#include <random>
#include <functional>
#include <atomic>
#include <algorithm>
#include <execution>

class TestBloom : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using Storage = ::jb::Storage< Policies, Pad >;
    using Bloom = typename Storage::PhysicalVolumeImpl::Bloom;
    using Key = typename Storage::Key;
    using KeyCharT = typename Key::CharT;

    Bloom filter_;
    std::unordered_set< std::basic_string< KeyCharT > > present_;
    std::unordered_set< std::basic_string< KeyCharT > > absent_;

    static constexpr size_t present_number = 10'000;
    static constexpr size_t absent_number = 1'000;


    auto generate( ) const
    {
        using namespace std;

        auto distr = uniform_int_distribution<>{};
        std::mt19937 rand{};

        auto r = static_cast< size_t >( distr( rand ) );
        auto length = max( min( Policies::PhysicalVolumePolicy::BloomPrecision, r ), size_t{ 2 } );
        basic_string< KeyCharT > s( length, KeyCharT{ '0' } );
        
        static const auto alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"s;

        for_each( execution::par, begin( s ), end( s ), [&]( auto & ch ){
            ch =  static_cast< KeyCharT >( alpha[ distr( rand ) % alpha.size( ) ] );
        } );

        return s;
    }


    auto split_to_keys( const std::basic_string< KeyCharT > & str )
    {
        using namespace std;

        assert( str.size( ) >= 2 );

        auto distr = uniform_int_distribution<>{};
        std::mt19937 rand{};
        auto r = static_cast< size_t >( distr( rand ) );
        auto pos = min( max( size_t{ 1 }, r % str.size( ) ), str.size( ) - 1 );
        return tuple{ Key{ str.substr( 0, pos ) }, Key{ str.substr( pos ) } };
    }


    TestBloom()
    {
        using namespace std;

        auto distr = uniform_int_distribution<>{};
        std::mt19937 rand{};

        while ( present_.size( ) < present_number )
        {
            auto str{ move( generate( ) ) };
            auto[ k1, k2 ] = split_to_keys( str );
            filter_.add( k1, k2 );
            present_.emplace( move( str ) );
        }

        while ( absent_.size( ) < absent_number )
        {
            if ( auto str = move( generate( ) );  !present_.count( str ) )
            {
                absent_.emplace( move( str ) );
            }
        }
    }
};


TEST_F( TestBloom, Overall_Takes_Long_Lime )
{
    using namespace std;

    atomic< size_t > positive_counter(0);

    for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & str ) {
        auto[ k1, k2 ] = split_to_keys( str );
        if ( filter_.test( k1, k2 ) )
        {
            positive_counter++;
        }
    } );

    EXPECT_LE( 0.99 * present_number, positive_counter.load() );

    atomic< size_t > negative_counter( 0 );

    for_each( execution::par, begin( absent_ ), end( absent_ ), [&] ( const auto & str ) {
        auto[ k1, k2 ] = split_to_keys( str );
        if ( !filter_.test( k1, k2 ) )
        {
            negative_counter++;
        }
    } );

    EXPECT_EQ( absent_number, negative_counter.load() );
}