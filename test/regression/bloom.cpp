#include <gtest/gtest.h>
#include <bloom.h>
#include <policies.h>
#include <unordered_set>
#include <random>
#include <functional>
#include <atomic>
#include <algorithm>
#include <execution>

class TestBloom : public ::testing::Test
{
protected:

    using Bloom = ::jb::Bloom< ::jb::DefaultPolicies >;
    using KeyValueT = typename ::jb::DefaultPolicies::KeyValueT;

    Bloom filter_;
    std::unordered_set< KeyValueT > present_;
    std::unordered_set< KeyValueT > absent_;

    static constexpr size_t present_number = 500'000;
    static constexpr size_t absent_number = 100'000;

    TestBloom()
    {
        using namespace std;

        auto distr = uniform_real_distribution<>{};
        std::mt19937 rand{};

        while ( present_.size() < present_number )
        {
            auto str = to_string( distr( rand ) );
            filter_.add( str );
            present_.emplace( move( str ) );
        }

        while ( absent_.size() < absent_number )
        {
            auto str = to_string( distr( rand ) );

            if ( present_.find( str ) == present_.end() )
            {
                absent_.emplace( move( str ) );
            }
        }
    }
};


TEST_F( TestBloom, Overall )
{
    using namespace std;

    atomic< size_t > positive_counter(0);

    for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & str ) {
        if ( filter_.test( str ) )
        {
            positive_counter++;
        }
    } );

    EXPECT_LE( 0.99 * present_number, positive_counter.load() );

    atomic< size_t > negative_counter( 0 );

    for_each( execution::par, begin( absent_ ), end( absent_ ), [&] ( const auto & str ) {

        if ( !filter_.test( str ) )
        {
            negative_counter++;
        }
    } );

    EXPECT_EQ( absent_number, negative_counter.load() );
}