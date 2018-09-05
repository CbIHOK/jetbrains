#include <gtest/gtest.h>
#include <storage.h>
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

    using Policies = ::jb::DefaultPolicy<>;
    using Storage = ::jb::Storage< Policies >;
    using RetCode = typename Storage::RetCode;
    using Bloom = typename Storage::PhysicalVolumeImpl::Bloom;
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using Key = typename Storage::Key;
    using KeyCharT = typename Key::CharT;
    using Digest = typename Bloom::Digest;
    using DigestPath = typename Bloom::DigestPath;

    std::mutex present_mutex_;
    std::unordered_set< std::basic_string< KeyCharT > > present_;
    std::mutex absent_mutex_;
    std::unordered_set< std::basic_string< KeyCharT > > absent_;

    void SetUp() override
    {
        using namespace std;

        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
    }

    void TearDown() override
    {
        using namespace std;

        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
    }

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
};


TEST_F( TestBloom, Store_Restore )
{
    using namespace std;

    constexpr size_t PresentNumber = 1'000;

    {
        StorageFile file( "TestBloom_Store_Restore.jb", true );
        ASSERT_EQ( RetCode::Ok, file.status() );

        auto bloom = make_shared< Bloom >( file );
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
        for_each( begin( present_ ), end( present_ ), [&] ( const auto & key_str )
        {
            Key key( key_str );

            DigestPath digests;
            EXPECT_NO_THROW( EXPECT_FALSE( bloom->test( 0, key, digests ) ); );
            EXPECT_NO_THROW( for ( auto digest : digests ) bloom->add_digest( digest ); );
        } );
    }

    // reopen storage and retrieve filter data
    {
        StorageFile file( "TestBloom_Store_Restore.jb", true );
        ASSERT_EQ( RetCode::Ok, file.status() );

        auto bloom = make_shared< Bloom >( file );
        EXPECT_EQ( RetCode::Ok, bloom->status() );

        atomic< size_t > positive_counter( 0 );
        for_each( execution::par, begin( present_ ), end( present_ ), [&] ( const auto & str ) {

            EXPECT_NO_THROW(

            Key key{ str };
            Bloom::DigestPath digests;
            if ( auto may_present = bloom->test( 0, key, digests ) )
            {
                positive_counter++;
            }

            );
        } );
        EXPECT_LE( 0.99 * PresentNumber, positive_counter.load() );
    }
}