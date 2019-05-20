#include <gtest/gtest.h>
#include <details/rare_write_frequent_read_mutex.h>
#include <thread>
#include <atomic>


struct rare_write_frequent_read_mutex_test : public ::testing::Test
{
    using mutex = jb::details::rare_write_frequent_read_mutex<>;
    using unique_lock = typename mutex::unique_lock<>;
    using shared_lock = typename mutex::shared_lock<>;
    using upgrade_lock = typename mutex::upgrade_lock<>;

    static constexpr size_t shared_locker_number = 100;

    inline static mutex mutex_;
    inline static size_t semaphore_ = 0;
    inline static std::atomic< bool > go_, stop_;

    static void shared_locker_fn()
    {
        while ( !go_.load( std::memory_order_acquire ) ) std::this_thread::yield();

        while ( !stop_.load( std::memory_order_acquire ) )
        {
            {
                shared_lock s_lock( mutex_ );
                semaphore_++;
                EXPECT_GE( shared_locker_number, semaphore_ );
                semaphore_--;
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
                semaphore_ += shared_locker_number;
                EXPECT_EQ( shared_locker_number, semaphore_ );
                semaphore_ -= shared_locker_number;
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
                semaphore_++;
                EXPECT_GE( shared_locker_number, semaphore_ );
                semaphore_--;

                {
                    upgrade_lock x_lock( s_lock );
                    semaphore_ += shared_locker_number;
                    EXPECT_EQ( shared_locker_number, semaphore_ );
                    semaphore_ -= shared_locker_number;
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

    std::list< std::thread > threads;
    for ( size_t i = 0; i < shared_locker_number; ++i )
    {
        threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::shared_locker_fn ) );
    }
    //threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::unique_locker_fn ) );
    //threads.push_back( std::thread( &rare_write_frequent_read_mutex_test::upgrade_locker_fn ) );

    go_.store( true, std::memory_order_release );
    std::this_thread::sleep_for( 100ms );
    stop_.store( true, std::memory_order_release );

    for ( auto & thread : threads ) thread.join();
}