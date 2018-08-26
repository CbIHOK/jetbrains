#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <unordered_set>
#include <random>
#include <functional>
#include <atomic>
#include <algorithm>
#include <execution>
#include <boost/container/static_vector.hpp>


class TestBloom : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using Storage = ::jb::Storage< Policies, Pad >;
    using RetCode = typename Storage::RetCode;
    using Bloom = typename Storage::PhysicalVolumeImpl::Bloom;
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using Key = typename Storage::Key;
    using KeyCharT = typename Key::CharT;
    using Digest = typename Bloom::Digest;

    template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

    static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;

    std::mutex present_mutex_;
    std::unordered_set< std::basic_string< KeyCharT > > present_;
    std::mutex absent_mutex_;
    std::unordered_set< std::basic_string< KeyCharT > > absent_;

    auto generate( std::mt19937 & rand ) const
    {
        using namespace std;

        auto distr = uniform_int_distribution<>{};

        basic_string< KeyCharT > s( 100, KeyCharT{ '0' } );
        
        static const auto alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"s;

        for ( size_t i = 0; i < 100; i++ )
        {
            if ( i % 5 == 0 )
            {
                s[ i ] = '/';
            }
            else
            {
                auto ch = static_cast< KeyCharT >( alpha[ distr( rand ) % alpha.size() ] );
                s[ i ] = ch;
            }
        };

        return s;
    }


    auto split_to_keys( const std::basic_string< KeyCharT > & str )
    {
        using namespace std;

        Key key( str );

        auto distr = uniform_int_distribution<>{};
        static std::mt19937 rand{};
        auto r = static_cast< size_t >( distr( rand ) ) % 10 + 1;

        auto rest = key;
        for ( size_t i = 0; i < r; i++ )
        {
            auto[ ok, prefix, suffix ] = rest.split_at_tile();
            assert( ok );
            rest = prefix;
        }
        
        auto[ superkey_ok, subkey ] = rest.is_superkey( key );
        assert( superkey_ok );
        
        return tuple{ rest, subkey };
    }
};


class DISABLED_TestBloom : public TestBloom
{
};


TEST_F( DISABLED_TestBloom, Long_long_test )
{
    using namespace std;

    constexpr size_t PresentNumber = 1'000'000;
    constexpr size_t AbsentNumber = 1'000'000;

    {
        cout << endl << "WARNING: the test is run on over 1,000,000 random keys and may take up to 20 mins" << endl;
        cout << endl << "Generating keys..." << endl;

        vector< pair< mt19937, future< void > > > generators( 64 );

        // generate present keys
        unsigned seed = 0;
        for ( auto & generator : generators )
        {
            generator.first.seed( seed++ );
            generator.second = async( launch::async, [&] { 
                while ( true )
                {
                    auto str = move( generate( generator.first ) );

                    scoped_lock l( present_mutex_ );

                    if ( present_.size() < PresentNumber )
                    {
                        present_.emplace( move( str ) );
                    }
                    else
                    {
                        break;
                    }
                }
            } );
        }
        for ( auto & generator : generators ){ generator.second.wait(); }

        // generate absent keys
        for ( auto & generator : generators )
        {
            generator.first.seed( seed++ );
            generator.second = async( launch::async, [&] {
                while ( true )
                {
                    auto str = move( generate( generator.first ) );
                    
                    scoped_lock l( absent_mutex_ );

                    if ( absent_.size() < AbsentNumber )
                    {
                        if ( !present_.count( str ) )
                        {
                            absent_.emplace( move( str ) );
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            } );
        }
        for ( auto & generator : generators ) { generator.second.wait(); }

        cout << endl << "Processing keys..." << endl;

        // create filter
        auto bloom = make_shared< Bloom >( nullptr );
        ASSERT_EQ( RetCode::Ok, bloom->status() );

        // through all the keys - berak them in digest and put into the filter
        for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & key_str )
        {
            Key key( key_str );

            size_t level = 0;

            if ( Key::root() != key )
            {
                auto rest = key;

                size_t level = 0;

                while ( rest.size() )
                {
                    auto[ split_ok, prefix, suffix ] = rest.split_at_head();
                    assert( split_ok );

                    auto[ trunc_ok, stem ] = prefix.cut_lead_separator();
                    assert( trunc_ok );

                    auto digest = Bloom::generate_digest( level, stem );
                    bloom->add_digest( digest );

                    rest = suffix;
                    level++;
                }
            }
        } );

        cout << endl << "Checking keys..." << endl;

        // check positive
        atomic< size_t > positive_counter( 0 );
        for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & str ) {
            auto[ k1, k2 ] = split_to_keys( str );
            static_vector< Digest, MaxTreeDepth > digests;
            if ( auto[ ret, may_present ] = bloom->test( k1, k2, digests ); may_present )
            {
                positive_counter++;
            }
        } );
        EXPECT_LE( 0.99 * PresentNumber, positive_counter.load() );

        // check negative
        atomic< size_t > negative_counter( 0 );
        for_each( execution::par, begin( absent_ ), end( absent_ ), [&] ( const auto & str ) {
            auto[ k1, k2 ] = split_to_keys( str );
            static_vector< Digest, MaxTreeDepth > digests;
            if ( auto[ ret, may_present ] = bloom->test( k1, k2, digests ); !may_present )
            {
                negative_counter++;
            }
        } );
        EXPECT_EQ( AbsentNumber, negative_counter.load() );
    }
}


TEST_F( TestBloom, Store_Restore )
{
    using namespace std;

    constexpr size_t PresentNumber = 1'000;

    {
        StorageFile file( "TestBloom_Store_Restore.jb", true );
        ASSERT_EQ( RetCode::Ok, file.status() );

        auto bloom = make_shared< Bloom >( &file );
        EXPECT_EQ( RetCode::Ok, bloom->status() );

        vector< pair< mt19937, future< void > > > generators( 64 );

        // generate present keys
        unsigned seed = 0;
        for ( auto & generator : generators )
        {
            generator.first.seed( seed++ );
            generator.second = async( launch::async, [&] {
                while ( true )
                {
                    auto str = move( generate( generator.first ) );

                    scoped_lock l( present_mutex_ );

                    if ( present_.size() < PresentNumber )
                    {
                        present_.emplace( move( str ) );
                    }
                    else
                    {
                        break;
                    }
                }
            } );
        }
        for ( auto & generator : generators ) { generator.second.wait(); }

        // through all the keys - berak them in digest and put into the filter
        for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & key_str )
        {
            Key key( key_str );

            size_t level = 1;

            if ( Key::root() != key )
            {
                auto rest = key;

                while ( rest.size() )
                {
                    auto[ split_ok, prefix, suffix ] = rest.split_at_head();
                    assert( split_ok );

                    auto[ trunc_ok, stem ] = prefix.cut_lead_separator();
                    assert( trunc_ok );

                    auto digest = Bloom::generate_digest( level, stem );
                    bloom->add_digest( digest );

                    rest = suffix;
                    level++;
                }
            }
        } );
    }
    // reopen storage and retrieve filter data
    {
        StorageFile file( "TestBloom_Store_Restore.jb", true );
        ASSERT_EQ( RetCode::Ok, file.status() );

        auto bloom = make_shared< Bloom >( &file );
        EXPECT_EQ( RetCode::Ok, bloom->status() );

        atomic< size_t > positive_counter( 0 );
        for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & str ) {
            auto[ k1, k2 ] = split_to_keys( str );
            static_vector< Digest, MaxTreeDepth > digests;
            if ( auto[ ret, may_present ] = bloom->test( k1, k2, digests ); may_present )
            {
                positive_counter++;
            }
        } );
        EXPECT_LE( 0.99 * PresentNumber, positive_counter.load() );
    }

    error_code ec;
    filesystem::remove( "TestBloom_Store_Restore.jb", ec );
}