#include <gtest/gtest.h>
#include <storage.h>
#include <tuple>
#include "policies.h"


struct TestPackedValuePolicy : public jb::DefaultPolicy<>
{
    using ValueT = std::variant< char, int32_t, uint64_t, float, double, std::string, std::wstring, std::pair< int, std::string > >;

    struct PhysicalVolumePolicy : public jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1 << 10;
    };
};


namespace jb
{
    template <>
    struct is_blob_type< std::pair< int, std::string > >
    {
        static constexpr bool value = true;
        using StreamCharT = char;
    };
}


class TestPackedValue : public ::testing::Test
{

protected:

    using Storage = ::jb::Storage< TestPackedValuePolicy >;
    using RetCode = typename Storage::RetCode;
    using Value = typename Storage::Value;
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using Transaction = typename StorageFile::Transaction;
    using PackedValue = typename Storage::PhysicalVolumeImpl::BTree::PackedValue;

    void SetUp() override
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
};


TEST_F( TestPackedValue, Char )
{
    StorageFile f{ "TestPackedValue_Char.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

    Value v{ 't' };

    auto t = f.open_transaction();
        
    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );
        
    EXPECT_FALSE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, Int )
{
    StorageFile f{ "TestPackedValue_Int.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

    Value v{ 1234 };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_FALSE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, Uint64 )
{
    StorageFile f{ "TestPackedValue_Uint64.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

    Value v{ (uint64_t)1234 };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_FALSE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, Float )
{
    StorageFile f{ "TestPackedValue_Float.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

        Value v{ 1234.f };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_FALSE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, Double )
{
    StorageFile f{ "TestPackedValue_Double.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

        Value v{ 1234. };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_FALSE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, String )
{
    StorageFile f{ "TestPackedValue_String.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

    Value v{ "1234" };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_TRUE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}


TEST_F( TestPackedValue, WString )
{
    StorageFile f{ "TestPackedValue_WString.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

        Value v{ L"1234" };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_TRUE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}

namespace jb
{
    std::ostream & operator << ( std::ostream & os, const std::pair< int, std::string > & v )
    {
        os << v.first;
        os << ";";
        os << v.second;
        return os;
    }

    std::istream & operator >> ( std::istream & is, std::pair< int, std::string > & v )
    {
        is >> v.first;
        is.get();
        is >> v.second;
        return is;
    }
}

TEST_F( TestPackedValue, Custom )
{
    StorageFile f{ "TestPackedValue_WString.jb", true };
    ASSERT_EQ( RetCode::Ok, f.status() );

    EXPECT_NO_THROW(

    Value v{ std::make_pair( 1234, "1234" ) };

    auto t = f.open_transaction();

    auto p = PackedValue::make_packed( t, v );

    t.commit();
    EXPECT_EQ( RetCode::Ok, f.status() );

    EXPECT_TRUE( p.is_blob() );

    auto u = p.unpack( f );
    EXPECT_EQ( RetCode::Ok, f.status() );
    EXPECT_EQ( v, u );

    );
}