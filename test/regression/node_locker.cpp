#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


class TestNodeLocker : public ::testing::Test
{
protected:

    using Storage = ::jb::Storage< ::jb::DefaultPolicy<> >;
    using PathLocker = typename Storage::PhysicalVolumeImpl::PathLocker;
    using NodeUid = typename Storage::PhysicalVolumeImpl::NodeUid;
    using PathLock = typename PathLocker::PathLock;

    PathLocker locker_;

    PathLock get_locked( NodeUid uid )
    {
        return locker_.lock( uid );
    }
};


TEST_F( TestNodeLocker, Overall )
{
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        PathLock lock = get_locked( 0 );
        EXPECT_FALSE( locker_.is_removable( 0 ) );
    }
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        PathLock lock = get_locked( 0 );
        {
            PathLock lock = get_locked( 0 );
            EXPECT_FALSE( locker_.is_removable( 0 ) );
        }
        EXPECT_FALSE( locker_.is_removable( 0 ) );
    }
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        PathLock lock = get_locked( 0 );
        {
            lock = get_locked( 1 );
            EXPECT_TRUE( locker_.is_removable( 0 ) );
            EXPECT_FALSE( locker_.is_removable( 1 ) );
        }
        EXPECT_TRUE( locker_.is_removable( 0 ) );
        EXPECT_FALSE( locker_.is_removable( 1 ) );
    }
    EXPECT_TRUE( locker_.is_removable( 0 ) );
    EXPECT_TRUE( locker_.is_removable( 1 ) );

}