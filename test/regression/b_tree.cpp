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

    bool is_leaf_element( BTree & node, size_t pos )
    {
        return node.links_[ pos ] == InvalidNodeUid && node.links_[ pos + 1 ] == InvalidNodeUid;
    }

public:

    ~TestBTree()
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

TEST_F( TestBTree, Serialization )
{
    NodeUid uid;

    ElementCollection etalon_elements{
        { Digest{ 0 }, Value{ ( uint32_t )0 }, 0, 0 },
        { Digest{ 2 }, Value{ 2.f }, 2, 2 },
        { Digest{ 3 }, Value{ 3. }, 3, 3 },
        { Digest{ 4 }, Value{ "4444" }, 4, 4 }
    };
    LinkCollection etalon_links{ 0, 2, 3, 4, 5 };

    {
        StorageFile f( "foo1000.jb", true );
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
        StorageFile f( "foo1000.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTreeCache c( &f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        BTree tree( uid, &f, &c );
        EXPECT_EQ( etalon_elements, elements( tree ) );
        EXPECT_EQ( etalon_links, links( tree ) );
    }
}


TEST_F( TestBTree, Insert_Find )
{
    using namespace std;

    StorageFile f( "foo6.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    BTreeCache c( &f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    auto[ rc, root ] = c.get_node( RootNodeUid );
    EXPECT_EQ( RetCode::Ok, rc );
    EXPECT_TRUE( root );

    for ( size_t i = 0; i < 1000; ++i )
    {
        string key = "jb_" + to_string( i );
        Digest digest = Bloom::generate_digest( 1, Key{ key } );

        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_FALSE( found );

        {
            auto target = bpath.back(); bpath.pop_back();

            auto[ rc, node ] = c.get_node( target.first );
            EXPECT_EQ( RetCode::Ok, rc );

            node->insert( target.second, bpath, digest, Value{ key }, 0, false );
        }
    }

    // collect leafs depths
    std::set< size_t > depth;

    for ( size_t i = 0; i < 1000; ++i )
    {
        string key = "jb_" + to_string( i );
        Digest digest = Bloom::generate_digest( 1, Key{ key } );

        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( found );

        {
            auto[ rc, node ] = c.get_node( bpath.back().first );
            EXPECT_EQ( RetCode::Ok, rc );

            node->value( bpath.back().second ) == Value{ key };

            if ( is_leaf_element( *node, bpath.back().second ) ) depth.insert( bpath.size() );
        }
    }

    // check that tree is balanced
    EXPECT_GE( 2, depth.size() );
}