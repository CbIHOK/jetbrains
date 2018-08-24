#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <mutex>


struct OtherPolicies : public ::jb::DefaultPolicies
{
    using KeyCharT = wchar_t;
    using ValueT = std::variant< uint32_t, uint64_t, float, double >;
};

struct SmallChunkPolicies : public ::jb::DefaultPolicies
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 32;
        static constexpr size_t BloomSize = 1024;
    };
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
    using SmallChunkStorage = jb::Storage< SmallChunkPolicies, Pad >;
    using SmallChunkStorageFile = typename SmallChunkStorage::PhysicalVolumeImpl::PhysicalStorage::StorageFile;
    using Transaction = typename StorageFile::Transaction;
    using SmallChunkTransaction = typename SmallChunkStorageFile::Transaction;
    using ostreambuf = typename SmallChunkStorageFile::ostreambuf;

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


TEST_F( TestStorageFile, Transaction_Lock )
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


TEST_F( TestStorageFile, Transaction_Rollback )
{
    using namespace std;

    SmallChunkStorageFile f{ std::filesystem::path{ "./foo3.jb" }, true };
    ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.creation_status() );

    {
        auto t = f.open_transaction();
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_writer();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        std::ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );
        EXPECT_EQ( SmallChunkStorageFile::RootChunkUid, t.get_first_written_chunk() );
    }
}