#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>
#include <mutex>


template < typename P >
struct OtherPolicy : public P
{
    using KeyCharT = wchar_t;
    using ValueT = std::variant< uint32_t, uint64_t, float, double >;
};


struct Chunk_31 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 31;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_32 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 32;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_33 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 33;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_64 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 64;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_512 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 512;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_2048 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 2048;
        static constexpr size_t BloomSize = 1024;
    };
};

struct Chunk_4096 : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t ChunkSize = 4096;
        static constexpr size_t BloomSize = 1024;
    };
};

template < typename P >
class TestStorageFile : public ::testing::Test
{

protected:
    using Policy = P;
    using Storage = ::jb::Storage< Policy >;
    using RetCode = typename Storage::RetCode;
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using ChunkUid = typename StorageFile::ChunkUid;
    using OtherStorage = jb::Storage< OtherPolicy< Policy > >;
    using OtherStorageFile = typename OtherStorage::PhysicalVolumeImpl::StorageFile;
    using Transaction = typename StorageFile::Transaction;
    using ostreambuf = typename StorageFile::ostreambuf;
    
public:

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

    auto & get_transaction_mutex( StorageFile & f )
    {
        return f.transaction_mutex_;
    }

    bool is_newly_created( const StorageFile & f ) const noexcept { return f.newly_created_; }
};

typedef ::testing::Types< Chunk_31, Chunk_32, Chunk_33, Chunk_64, Chunk_512, Chunk_2048, Chunk_4096 > TestingPolicies;
TYPED_TEST_CASE( TestStorageFile, TestingPolicies );


TYPED_TEST( TestStorageFile, CreateNew_OpenExisting )
{
    {
        StorageFile f{ std::filesystem::path{ "CreateNew_OpenExisting.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f.status() );
        EXPECT_TRUE( is_newly_created( f ) );
    }

    {
        StorageFile f{ std::filesystem::path{ "CreateNew_OpenExisting.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f.status() );
        EXPECT_FALSE( is_newly_created( f ) );
    }
}


TYPED_TEST( TestStorageFile, LockingFile )
{
    {
        StorageFile f1{ std::filesystem::path{ "LockingFile.jb" } };
        EXPECT_EQ( RetCode::Ok, f1.status() );

        StorageFile f2{ std::filesystem::path{ "LockingFile.jb" } };
        EXPECT_EQ( RetCode::AlreadyOpened, f2.status() );
    }

    StorageFile f{ std::filesystem::path{ "LockingFile.jb" } };
    EXPECT_EQ( RetCode::Ok, f.status() );
}


TYPED_TEST( TestStorageFile, Compatibility )
{
    {
        StorageFile f{ std::filesystem::path{ "Compatibility.jb" }, true };
        ASSERT_EQ( RetCode::Ok, f.status() );
    }

    {
        OtherStorageFile f{ std::filesystem::path{ "Compatibility.jb" }, true };
        ASSERT_EQ( OtherStorage::RetCode::IncompatibleFile, f.status() );
    }
}


TYPED_TEST( TestStorageFile, Transaction_Lock )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "Transaction_Lock.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    {
        auto t = f.open_transaction();
        EXPECT_EQ( RetCode::Ok, t.status() );
        EXPECT_EQ( false, get_transaction_mutex( f ).try_lock() );
    }

    EXPECT_EQ( true, get_transaction_mutex( f ).try_lock() );
    get_transaction_mutex( f ).unlock();
}


TYPED_TEST( TestStorageFile, Transaction_Rollback )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "Transaction_Rollback.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    ChunkUid uid;
    {
        auto t = f.open_transaction();
        ASSERT_EQ( RetCode::Ok, t.status() );

        auto b = t.get_chain_writer< char >();
        EXPECT_EQ( RetCode::Ok, t.status() );

        ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();
        uid = t.get_first_written_chunk();

        EXPECT_EQ( RetCode::Ok, t.status() );
    }

    {
        auto b = f.get_chain_reader< char >( uid );
        istream is( &b );
        string str;
        is >> str;

        EXPECT_EQ( ""s, str );
    }
}

TYPED_TEST( TestStorageFile, Transaction_Commit )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "Transaction_Commit.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );
    
    const size_t count = 10;

    ChunkUid uid[ count ];

    std::array< double, count > factor = {
        0.7, 1.0, 1.4, 1.7, 2.0,
        2.3, 3.1, 4.2, 4.7, 4.9
    };

    {
        auto t = f.open_transaction();
        ASSERT_EQ( RetCode::Ok, t.status() );

        for ( size_t i = 0; i < count; ++i )
        {
            size_t sz = static_cast< size_t >( Policy::PhysicalVolumePolicy::ChunkSize * factor[ i ] );
            string data( sz, '0' + char( i ) );

            auto b = t.get_chain_writer< char >();
            EXPECT_EQ( RetCode::Ok, t.status() );

            ostream os( &b );
            os << data;
            os.flush();
            uid[ i ] = t.get_first_written_chunk();
        }

        EXPECT_EQ( RetCode::Ok, t.status() );
        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    {
        for ( size_t i = 0; i < count; ++i )
        {
            size_t sz = static_cast< size_t >( Policy::PhysicalVolumePolicy::ChunkSize * factor[ i ] );
            string data( sz, '0' + char( i ) );
            string read_data;

            auto b = f.get_chain_reader< char >( uid[ i ] );
            istream is( &b );
            string str;
            is >> read_data;

            EXPECT_EQ( data, read_data );
        }
    }
}


TYPED_TEST( TestStorageFile, Transaction_RollbackOnOpen )
{
    using namespace std;

    ChunkUid uid;
    {
        StorageFile f{ std::filesystem::path{ "Transaction_RollbackOnOpen.jb" }, true };
        ASSERT_EQ( RetCode::Ok, f.status() );

        auto t = f.open_transaction();
        ASSERT_EQ( RetCode::Ok, t.status() );

        auto b = t.get_chain_writer< char >();
        EXPECT_EQ( RetCode::Ok, t.status() );

        ostream os( &b );
        os << "abcdefghijklmnopqrstuvwxyz";
        os.flush();
        uid = t.get_first_written_chunk();

        EXPECT_EQ( RetCode::Ok, t.status() );
    }

    {
        StorageFile f{ std::filesystem::path{ "Transaction_RollbackOnOpen.jb" }, true };
        ASSERT_EQ( RetCode::Ok, f.status() );

        auto b = f.get_chain_reader< char >( uid );
        istream is( &b );
        string str;
        is >> str;

        EXPECT_EQ( ""s, str );
    }
}


TYPED_TEST( TestStorageFile, Overwriting )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "Overwriting.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    const size_t count = 10;
    ChunkUid uid[ count ];

    std::array< double, count > factor = {
        0.7, 1.0, 1.4, 1.7, 2.0,
        2.3, 3.1, 4.2, 4.7, 4.9
    };

    {
        auto t = f.open_transaction();
        ASSERT_EQ( RetCode::Ok, t.status() );

        for ( size_t i = 0; i < count; ++i )
        {
            size_t sz = static_cast< size_t >( Policy::PhysicalVolumePolicy::ChunkSize * factor[ i ] );
            string data( sz, '0' + char( i ) );

            auto b = t.get_chain_writer< char >();
            EXPECT_EQ( RetCode::Ok, t.status() );

            ostream os( &b );
            os << data;
            os.flush();
            uid[ i ] = t.get_first_written_chunk();
        }

        EXPECT_EQ( RetCode::Ok, t.status() );
        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    for ( size_t i = 0; i < count; ++i )
    {
        auto t = f.open_transaction();
        ASSERT_EQ( RetCode::Ok, t.status() );

        size_t sz = static_cast< size_t >( Policy::PhysicalVolumePolicy::ChunkSize * factor[ 9 - i ] );
        string data( sz, 'A' + char( i ) );

        auto [ rc, b ] = t.get_chain_overwriter< char >( uid[ i ] );
        EXPECT_EQ( RetCode::Ok, rc );

        ostream os( &b );
        os << data;
        os.flush();

        EXPECT_EQ( RetCode::Ok, t.status() );
        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    for ( size_t i = 0; i < count; ++i )
    {
        size_t sz = static_cast< size_t >( Policy::PhysicalVolumePolicy::ChunkSize * factor[ 9 - i ] );
        string data( sz, 'A' + char( i ) );
        string read_data;

        auto b = f.get_chain_reader< char >( uid[ i ] );
        istream is( &b );
        string str;
        is >> read_data;

        EXPECT_EQ( data, read_data );
    }
}


TYPED_TEST( TestStorageFile, Sequent_Overwriting )
{
    using namespace std;

    StorageFile f{ std::filesystem::path{ "Sequent_Overwriting.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    auto t = f.open_transaction();
    ASSERT_EQ( RetCode::Ok, t.status() );

    {
        auto[ rc, b ] = t.get_chain_overwriter< char >( StorageFile::RootChunkUid );
        EXPECT_EQ( RetCode::Ok, rc );
    }

    {
        auto[ rc, b ] = t.get_chain_overwriter< char >( StorageFile::RootChunkUid );
        EXPECT_EQ( RetCode::UnknownError, rc );
    }
}


TYPED_TEST( TestStorageFile, GarbageCollector )
{
    using namespace std;

    ChunkUid uid_0, uid_1, uid_2, uid_3;


    StorageFile f{ std::filesystem::path{ "GarbageCollector.jb" }, true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    // write 3 chains
    {
        auto t = f.open_transaction();
        EXPECT_EQ( RetCode::Ok, t.status() );

        {
            auto b = t.get_chain_writer< char >();

            ostream os( &b );
            os << "0000000";
            os.flush();

            EXPECT_EQ( RetCode::Ok, t.status() );

            uid_0 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer< char >();

            ostream os( &b );
            os << "1111111111";
            os.flush();

            EXPECT_EQ( RetCode::Ok, t.status() );

            uid_1 = t.get_first_written_chunk();
        }
        {
            auto b = t.get_chain_writer< char >();

            ostream os( &b );
            os << "2222222222222";
            os.flush();

            EXPECT_EQ( RetCode::Ok, t.status() );

            uid_2 = t.get_first_written_chunk();
        }

        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    // erase 2nd
    {
        auto t = f.open_transaction();
        EXPECT_EQ( RetCode::Ok, t.status() );

        auto b = t.erase_chain( uid_1 );
        EXPECT_EQ( RetCode::Ok, t.status() );

        // now 2nd chain marked as free space
        EXPECT_EQ( RetCode::Ok, t.commit() );

    }

    // add one more chain
    {
        auto t = f.open_transaction();
        EXPECT_EQ( RetCode::Ok, t.status() );

        auto b = t.get_chain_writer< char >();

        ostream os( &b );
        os << "3333333333333333333333333333333333333333333333333";
        os.flush();

        EXPECT_EQ( RetCode::Ok, t.status() );

        // make sure that new chain utilized space from erased one
        uid_3 = t.get_first_written_chunk();
        EXPECT_LE( uid_0, uid_3 );
        EXPECT_LE( uid_3, uid_2 );

        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    {
        string str;

        auto b = f.get_chain_reader< char >( uid_3 );
        istream is( &b );
        is >> str;

        EXPECT_EQ( "3333333333333333333333333333333333333333333333333"s, str );
    }
}
