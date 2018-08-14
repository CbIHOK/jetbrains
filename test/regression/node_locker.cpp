#include <gtest/gtest.h>
#include <storage.h>

class TestNodeLocker : public ::testing::Test
{
protected:

    using Storage = ::jb::Storage< ::jb::DefaultPolicies, ::jb::DefaultPolicies >;
    using NodeLocker = typename Storage::PhysicalVolumeImpl::NodeLocker;
    using NodeUid = typename Storage::PhysicalVolumeImpl::NodeUid;
    using NodeLock = typename NodeLocker::NodeLock;

    NodeLocker locker_;

    NodeLock get_locked( NodeUid uid )
    {
        return locker_.lock_node( uid );
    }
};


TEST_F( TestNodeLocker, Overall )
{
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        NodeLock lock = get_locked( 0 );
        EXPECT_FALSE( locker_.is_removable( 0 ) );
    }
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        NodeLock lock = get_locked( 0 );
        {
            NodeLock lock = get_locked( 0 );
            EXPECT_FALSE( locker_.is_removable( 0 ) );
        }
        EXPECT_FALSE( locker_.is_removable( 0 ) );
    }
    EXPECT_TRUE( locker_.is_removable( 0 ) );

    {
        NodeLock lock = get_locked( 0 );
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