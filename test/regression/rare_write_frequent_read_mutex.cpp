#include <gtest/gtest.h>
#include <details/rare_write_frequent_read_mutex.h>
#include <thread>
#include <atomic>
#include <array>


struct rare_write_frequent_read_mutex_test : public ::testing::Test
{
    using mutex = jb::details::rare_write_frequent_read_mutex< 31 >;
    using unique_lock = typename mutex::unique_lock< 1024 >;
    using shared_lock = typename mutex::shared_lock< 1024 >;
    using upgrade_lock = typename mutex::upgrade_lock< 1024 >;

    static constexpr size_t shared_locker_number = 100;
    static constexpr size_t test_array_size = 1024;

    inline static std::atomic< bool > go_, stop_;
    inline static std::atomic< size_t > fill_, shared_count_, exclusive_count_;
    inline static mutex mutex_;
    inline static std::array< size_t, test_array_size > test_array_;

    static void shared_locker_fn()
    {
        while ( !go_.load( std::memory_order_acquire ) ) std::this_thread::yield();

        while ( !stop_.load( std::memory_order_acquire ) )
        {
            {
                shared_lock s_lock( mutex_ );
                shared_count_.fetch_add( 1, std::memory_order_acq_rel );
                auto [ min, max ] = std::minmax_element( test_array_.begin(), test_array_.end() );
                EXPECT_EQ( *min, *max );
            }

            std::this_thread::yield();
        }
    }

    static void unique_locker_fn()
    {
        while ( !go_.load( std::memory_order_acquire ) );

        while ( !stop_.load( std::memory_order_acquire ) )
        {
            {
                unique_lock x_lock( mutex_ );
                exclusive_count_.fetch_add( 1, std::memory_order_acq_rel );
                test_array_.fill( fill_.fetch_add( 1, std::memory_order_acq_rel ) );
            }

            std::this_thread::yield();
        }
    }

    static void upgrade_locker_fn()
    {
        while ( !go_.load( std::memory_order_acquire ) );

        while ( !stop_.load( std::memory_order_acquire ) )
        {
            {
                shared_lock s_lock( mutex_ );
                shared_count_.fetch_add( 1, std::memory_order_acq_rel );
                auto[ min, max ] = std::minmax_element( test_array_.begin(), test_array_.end() );
                EXPECT_EQ( *min, *max );
                {
                    upgrade_lock x_lock( s_lock );
                    exclusive_count_.fetch_add( 1, std::memory_order_acq_rel );
                    test_array_.fill( fill_.fetch_add( 1, std::memory_order_acq_rel ) );
                }
            }

            std::this_thread::yield();
        }
    }
};

TEST_F( rare_write_frequent_read_mutex_test, consistency )
{
    using namespace std::literals;

    go_.store( false, std::memory_order_release );
    stop_.store( false, std::memory_order_release );
    shared_count_.store( 0, std::memory_order_release );
    exclusive_count_.store( 0, std::memory_order_release );
    test_array_.fill( fill_.fetch_add( 1, std::memory_order_acq_rel ) );

    std::list< std::thread > threads;
    for ( size_t i = 0; i < shared_locker_number; ++i )
    {
        threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::shared_locker_fn ) );
    }
    threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::unique_locker_fn ) );
    threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::upgrade_locker_fn ) );

    go_.store( true, std::memory_order_release );
    std::this_thread::sleep_for( 30000ms );
    stop_.store( true, std::memory_order_release );

    for ( auto & thread : threads ) thread.join();
    EXPECT_LT( 0U, shared_count_.load( std::memory_order_acquire ) );
    EXPECT_LT( 0U, exclusive_count_.load( std::memory_order_acquire ) );
}