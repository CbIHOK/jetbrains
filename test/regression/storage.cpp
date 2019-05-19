#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <filesystem>


using namespace jb;


struct TestStorage : public ::testing::Test
{
    struct VirtualVolume
    {
        using Key = void;
        using Value = void;
        using MountPoint = void;

        RetCode status_ = Ok;
        size_t uid_;
        std::atomic< size_t > uid_holder_;

        VirtualVolume() : uid_( uid_holder_.fetch_add( 1 ) ) {}
        RetCode status() { return status_; }
    };

    struct PhysicalVolume
    {
        RetCode status_ = Ok;
        size_t uid_;
        std::atomic< size_t > uid_holder_;
        std::filesystem::path path_;
        size_t priority_;

        PhysicalVolume( const std::filesystem::path & path, size_t priority ) : uid_( uid_holder_.fetch_add( 1 ) ), path_( path ), priority_( priority ) {}
        RetCode status() { return status_; }
    };

    struct TestHooks
    {
        using VirtualVolumeT = VirtualVolume;
        using PhysicalVolumeT = PhysicalVolume;
    };

    using Policies = DefaultPolicy<>;
    using Storage = ::jb::Storage< Policies, TestHooks >;
};


TEST_F( TestStorage, VirtualVolume )
{
    std::tuple < jb::RetCode, std::weak_ptr< VirtualVolume > > ret;
    EXPECT_NO_THROW( ret = Storage::open_virtual_volume() );

    EXPECT_EQ( Ok, std::get< jb::RetCode >( ret ) );
    std::weak_ptr< VirtualVolume > vv = std::get< std::weak_ptr< VirtualVolume > >( ret );

    auto lock = vv.lock();
    EXPECT_GT( lock.use_count(), 1 );

    EXPECT_NO_THROW( EXPECT_EQ( Ok, Storage::close( vv ) ) );
    EXPECT_EQ( lock.use_count(), 1 );
    EXPECT_NO_THROW( EXPECT_EQ( InvalidHandle, Storage::close( vv ) ) );
}


TEST_F( TestStorage, PhysicalVolume )
{
    std::filesystem::path path = " A path";
    size_t priority = 7;

    std::tuple < jb::RetCode, std::weak_ptr< PhysicalVolume > > ret;
    EXPECT_NO_THROW( ret = Storage::open_physical_volume( path, priority ) );

    EXPECT_EQ( Ok, std::get< jb::RetCode >( ret ) );
    std::weak_ptr< PhysicalVolume > pv = std::get< std::weak_ptr< PhysicalVolume > >( ret );

    auto lock = pv.lock();
    EXPECT_GT( lock.use_count(), 1 );
    EXPECT_EQ( path, lock->path_ );
    EXPECT_EQ( priority, lock->priority_ );

    EXPECT_NO_THROW( EXPECT_EQ( Ok, Storage::close( pv ) ) );
    EXPECT_EQ( lock.use_count(), 1 );
    EXPECT_NO_THROW( EXPECT_EQ( InvalidHandle, Storage::close( pv ) ) );
}


TEST_F( TestStorage, CloseAll )
{
    auto [rc_1, vv_1 ] = Storage::open_virtual_volume();
    auto[ rc_2, vv_2 ] = Storage::open_virtual_volume();
    auto[ rc_3, vv_3 ] = Storage::open_virtual_volume();
    auto[ rc_4, pv_1 ] = Storage::open_physical_volume( "foo", 1 );
    auto[ rc_5, pv_2 ] = Storage::open_physical_volume( "boo", 2 );

    EXPECT_NO_THROW( Storage::close_all() );

    EXPECT_EQ( 0, vv_1.use_count() );
    EXPECT_EQ( 0, vv_2.use_count() );
    EXPECT_EQ( 0, vv_3.use_count() );
    EXPECT_EQ( 0, pv_1.use_count() );
    EXPECT_EQ( 0, pv_2.use_count() );
}
