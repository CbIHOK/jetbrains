#include <gtest/gtest.h>
#include <storage.h>
#include <policies.h>


struct TestBTreePolicy : public ::jb::DefaultPolicies
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies::PhysicalVolumePolicy
    {
        static constexpr size_t BTreeMinPower = 3;
    };
};


class TestBTree : public ::testing::Test
{

protected:

    using Storage = jb::Storage< TestBTreePolicy >;
    using Key = typename Storage::Key;
    using RetCode = typename Storage::RetCode;
    using Value = typename Storage::Value;
    using BTree = typename Storage::PhysicalVolumeImpl::BTree;
    using Digest = typename BTree::Digest;
    using Element = typename BTree::Element;
    using NodeUid = typename BTree::NodeUid;
    using ElementCollection = typename BTree::ElementCollection;
    using LinkCollection = typename BTree::LinkCollection;
    using StorageFile = Storage::PhysicalVolumeImpl::StorageFile;
    using BTreeCache = Storage::PhysicalVolumeImpl::BTreeCache;
    using Bloom = Storage::PhysicalVolumeImpl::Bloom;
    using BTreePath = typename BTree::BTreePath;

    static constexpr auto RootNodeUid = BTree::RootNodeUid;
    static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;

    static auto & elements( BTree & t ) noexcept { return t.elements_; }
    static auto & links( BTree & t ) noexcept { return t.links_; }
    static auto save( BTree & tree, StorageFile::Transaction & t ) { return tree.save( t ); }
};

TEST_F( TestBTree, Serialization )
{
    NodeUid uid;

    ElementCollection etalon_elements{
        { Digest{ 0 }, Value{ ( uint32_t )0 }, 0, 0 },
        { Digest{ 1 }, Value{ ( uint64_t )0 }, 1, 1 },
        { Digest{ 2 }, Value{ 2.f }, 2, 2 },
        { Digest{ 3 }, Value{ 3. }, 3, 3 },
        { Digest{ 4 }, Value{ "4444" }, 4, 4 }
    };
    LinkCollection etalon_links{ 0, 1, 2, 3, 4, 5 };

    {
        StorageFile f( "foo.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTreeCache c( &f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTree tree( InvalidNodeUid, &f, &c );
        elements( tree ) = etalon_elements;
        links( tree ) = etalon_links;

        auto t = f.open_transaction();
        EXPECT_NO_THROW( save( tree, t ) );
        EXPECT_EQ( RetCode::Ok, t.status() );
        uid = tree.uid();
        EXPECT_NE( InvalidNodeUid, uid );
        EXPECT_EQ( RetCode::Ok, t.commit() );
    }

    {
        StorageFile f( "foo.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTreeCache c( &f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTree tree( uid, &f, &c );
        EXPECT_EQ( etalon_elements, elements( tree ) );
        EXPECT_EQ( etalon_links, links( tree ) );
    }
}


TEST_F( TestBTree, Find_Insert )
{
    using namespace std;

    StorageFile f( "foo.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    BTreeCache c( &f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    auto[ rc, root ] = c.get_node( RootNodeUid );
    EXPECT_EQ( RetCode::Ok, rc );
    EXPECT_TRUE( root );

    for ( size_t i = 0; i < 1000; ++i )
    {
        string key = to_string( i );
        Digest digest = Bloom::generate_digest( 1, Key{ key } );

        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_FALSE( found );
    }
}