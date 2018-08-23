#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <mutex>


struct OtherPolicies : public ::jb::DefaultPolicies
{
    using KeyCharT = wchar_t;
    using ValueT = std::variant< uint32_t, uint64_t, float, double >;
};


class TestStorageFile : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using Storage = ::jb::Storage<Policies, Pad>;
    using RetCode = typename Storage::RetCode;
    using StorageFile = typename Storage::PhysicalVolumeImpl::PhysicalStorage::StorageFile;
    using OtherStorage = jb::Storage< OtherPolicies, Pad >;
    using OtherStorageFile = typename OtherStorage::PhysicalVolumeImpl::PhysicalStorage::StorageFile;
    using Transaction = typename StorageFile::Transaction;

    TestStorageFile()
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

    auto & get_transaction_mutex( StorageFile & f )
    {
        return f.transaction_mutex_;
    }

};


TEST_F( TestStorageFile, CreateNew )
{
    {
        StorageFile f{ std::filesystem::path{ "./foo.jb" }, false };
        EXPECT_EQ( RetCode::UnableToOpen, f.creation_status() );
    }

    {
        StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f.creation_status() );
        EXPECT_TRUE( f.newly_created() );
    }
}


TEST_F( TestStorageFile, LockingFile )
{
    {
        StorageFile f1{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f1.creation_status() );

        StorageFile f2{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::AlreadyOpened, f2.creation_status() );
    }

    StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    EXPECT_EQ( RetCode::Ok, f.creation_status() );
}



TEST_F( TestStorageFile, Compatibility )
{
    {
        StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        ASSERT_EQ( RetCode::Ok, f.creation_status() );
    }

    {
        OtherStorageFile f{ std::filesystem::path{ "./foo.jb" }, false };
        ASSERT_EQ( OtherStorage::RetCode::IncompatibleFile, f.creation_status() );
    }
}


TEST_F( TestStorageFile, OpenTransaction )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.creation_status() );

    {
        auto t = f.open_transaction();
        EXPECT_EQ( RetCode::Ok, t.status() );
        EXPECT_EQ( false, get_transaction_mutex( f ).try_lock() );
    }

    EXPECT_EQ( true, get_transaction_mutex( f ).try_lock() );
    get_transaction_mutex( f ).unlock();
}