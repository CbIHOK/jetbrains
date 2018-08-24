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
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using OtherStorage = jb::Storage< OtherPolicies, Pad >;
    using OtherStorageFile = typename OtherStorage::PhysicalVolumeImpl::StorageFile;
    using SmallChunkStorage = jb::Storage< SmallChunkPolicies, Pad >;
    using SmallChunkStorageFile = typename SmallChunkStorage::PhysicalVolumeImpl::StorageFile;
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
        EXPECT_EQ( RetCode::UnableToOpen, f.status() );
    }

    {
        StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f.status() );
        EXPECT_TRUE( f.newly_created() );
    }
}


TEST_F( TestStorageFile, LockingFile )
{
    {
        StorageFile f1{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f1.status() );

        StorageFile f2{ std::filesystem::path{ "./foo.jb" }, true };
        EXPECT_EQ( RetCode::AlreadyOpened, f2.status() );
    }

    StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    EXPECT_EQ( RetCode::Ok, f.status() );
}


TEST_F( TestStorageFile, Compatibility )
{
    {
        StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        ASSERT_EQ( RetCode::Ok, f.status() );
    }

    {
        OtherStorageFile f{ std::filesystem::path{ "./foo.jb" }, false };
        ASSERT_EQ( OtherStorage::RetCode::IncompatibleFile, f.status() );
    }
}


TEST_F( TestStorageFile, Transaction_Lock )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

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

    SmallChunkStorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

    {
        auto t = f.open_transaction();
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_writer();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );
        EXPECT_EQ( SmallChunkStorageFile::RootChunkUid, t.get_first_written_chunk() );
    }

    {
        auto b = f.get_chain_reader( SmallChunkStorageFile::RootChunkUid );
        istream is( &b );
        string str;
        is >> str;

        EXPECT_EQ( ""s, str );
    }
}

TEST_F( TestStorageFile, Transaction_Commit )
{
    using namespace std;

    SmallChunkStorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
    ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

    {
        auto t = f.open_transaction();
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_writer();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );
        EXPECT_EQ( SmallChunkStorageFile::RootChunkUid, t.get_first_written_chunk() );
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.commit() );
    }

    {
        auto b = f.get_chain_reader( SmallChunkStorageFile::RootChunkUid );
        istream is( &b );
        string str;
        is >> str;

        EXPECT_EQ( "abcdefghijklmnopqrstuvwxyz"s, str );
    }
}


TEST_F( TestStorageFile, Transaction_RollbackOnOpen )
{
    using namespace std;

    {
        SmallChunkStorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

        auto t = f.open_transaction();
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_writer();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );
        EXPECT_EQ( SmallChunkStorageFile::RootChunkUid, t.get_first_written_chunk() );
    }

    {
        SmallChunkStorageFile f{ std::filesystem::path{ "./foo.jb" }, true };
        ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

        auto b = f.get_chain_reader( SmallChunkStorageFile::RootChunkUid );
        istream is( &b );
        string str;
        is >> str;

        EXPECT_EQ( ""s, str );
    }
}


TEST_F( TestStorageFile, Overwriting )
{
    using namespace std;

    SmallChunkStorageFile::ChunkUid uid_0, uid_1, uid_2;


    SmallChunkStorageFile f{ std::filesystem::path{ "./foo4.jb" }, true };
    ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

    {
        auto t = f.open_transaction();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "0000000";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_0 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "1111111111";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_1 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "2222222222222";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_2 = t.get_first_written_chunk();
        }

        t.commit();
    }

    {
        auto t = f.open_transaction();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_overwriter( uid_1 );
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        ostream os( &b );
        os << "44444444444444444444444";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        t.commit();
    }

    {
        string str;

        auto b = f.get_chain_reader( uid_0 );
        istream is( &b );
        is >> str;

        EXPECT_EQ( "0000000"s, str );
    }
    {
        string str;

        auto b = f.get_chain_reader( uid_1 );
        istream is( &b );
        is >> str;

        EXPECT_EQ( "44444444444444444444444"s, str );
    }
    {
        string str;

        auto b = f.get_chain_reader( uid_2 );
        istream is( &b );
        is >> str;

        EXPECT_EQ( "2222222222222"s, str );
    }
}


TEST_F( TestStorageFile, GarbageCollector )
{
    using namespace std;

    SmallChunkStorageFile::ChunkUid uid_0, uid_1, uid_2, uid_3;


    SmallChunkStorageFile f{ std::filesystem::path{ "./foo4.jb" }, true };
    ASSERT_EQ( SmallChunkStorage::RetCode::Ok, f.status() );

    // write 3 chains
    {
        auto t = f.open_transaction();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "0000000";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_0 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "1111111111";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_1 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer();

            ostream os( &b );
            os << "2222222222222";
            os.flush();

            EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

            uid_2 = t.get_first_written_chunk();
        }

        t.commit();
    }

    // erase 2nd
    {
        auto t = f.open_transaction();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.erase_chain( uid_1 );
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        t.commit(); // now 2nd chain marked as free space
    }

    // add one more chain
    {
        auto t = f.open_transaction();
        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        auto b = t.get_chain_writer();

        ostream os( &b );
        os << "3333333333333333333333333333333333333333333333333";
        os.flush();

        EXPECT_EQ( SmallChunkStorage::RetCode::Ok, t.status() );

        // make sure that new chain utilized space from erased one
        uid_3 = t.get_first_written_chunk();
        EXPECT_LE( uid_0, uid_3 );
        EXPECT_LE( uid_3, uid_2 );

        t.commit();
    }

    {
        string str;

        auto b = f.get_chain_reader( uid_3 );
        istream is( &b );
        is >> str;

        EXPECT_EQ( "3333333333333333333333333333333333333333333333333"s, str );
    }
}
