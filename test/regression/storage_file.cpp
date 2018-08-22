#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>

class TestStorageFile : public ::testing::Test
{

protected:

    using Policies = ::jb::DefaultPolicies;
    using Pad = ::jb::DefaultPad;
    using Storage = ::jb::Storage<Policies, Pad>;
    using RetCode = typename Storage::RetCode;
    using StorageFile = typename Storage::PhysicalVolumeImpl::PhysicalStorage::StorageFile;

    TestStorageFile()
    {
        using namespace std;
        //std::error_code ec;
        //std::filesystem::remove_all( "./*.jb", ec );
        //std::cout << ec.message() << std::endl;

        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
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


TEST_F( TestStorageFile, Locking )
{
    {
        StorageFile f1{ std::filesystem::path{ "./abcdefghijk.jb" }, true };
        EXPECT_EQ( RetCode::Ok, f1.creation_status() );

        StorageFile f2{ std::filesystem::path{ "./abcdefghijk.jb" }, true };
        EXPECT_EQ( RetCode::AlreadyOpened, f2.creation_status() );
    }

    StorageFile f{ std::filesystem::path{ "./abcdefghijk.jb" }, true };
    EXPECT_EQ( RetCode::Ok, f.creation_status() );
}



