#include <gtest/gtest.h>
#include <storage.h>
#include "policies.h"


struct TBT_Min : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 3;
        static constexpr size_t ChunkSize = 32;
    };
};


struct TBT_Odd : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 4;
        static constexpr size_t ChunkSize = 2048;
    };
};


struct TBT_Prime : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 5;
    };
};


struct TBT_Regular : public ::jb::DefaultPolicies<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicies<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 1024;
    };
};


template < typename Policy >
class TestBTree : public ::testing::Test
{

protected:

    using Storage = jb::Storage< Policy >;
    using Key = typename Storage::Key;
    using RetCode = typename Storage::RetCode;
    using Value = typename Storage::Value;
    using BTree = typename Storage::PhysicalVolumeImpl::BTree;
    using Digest = typename BTree::Digest;
    using Element = typename BTree::Element;
    using NodeUid = typename BTree::NodeUid;
    using ElementCollection = typename BTree::ElementCollection;
    using LinkCollection = typename BTree::LinkCollection;
    using StorageFile = typename Storage::PhysicalVolumeImpl::StorageFile;
    using BTreeCache = typename Storage::PhysicalVolumeImpl::BTreeCache;
    using Bloom = typename Storage::PhysicalVolumeImpl::Bloom;
    using BTreePath = typename BTree::BTreePath;
    using Transaction = typename StorageFile::Transaction;

    static constexpr auto RootNodeUid = BTree::RootNodeUid;
    static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;

    static auto & elements( BTree & t ) noexcept { return t.elements_; }
    static auto & links( BTree & t ) noexcept { return t.links_; }
    static auto save( BTree & tree, Transaction & t ) { return tree.save( t ); }

    bool is_leaf_element( BTree & node, size_t pos )
    {
        return node.links_[ pos ] == InvalidNodeUid && node.links_[ pos + 1 ] == InvalidNodeUid;
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


typedef ::testing::Types< TBT_Min, TBT_Odd, TBT_Prime, TBT_Regular > TestingPolicies;
TYPED_TEST_CASE( TestBTree, TestingPolicies );


//TYPED_TEST( TestBTree, Serialization )
//{
//    NodeUid uid;
//
//    ElementCollection etalon_elements{
//        { Digest{ 0 }, Value{ ( uint32_t )0 }, 0, 0 },
//        { Digest{ 2 }, Value{ 2.f }, 2, 2 },
//        { Digest{ 3 }, Value{ 3. }, 3, 3 },
//        { Digest{ 4 }, Value{ "4444" }, 4, 4 }
//    };
//    LinkCollection etalon_links{ 0, 2, 3, 4, 5 };
//
//    {
//        StorageFile f( "serialization.jb", true );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTreeCache c( &f );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTree tree( InvalidNodeUid, &f, &c );
//        links( tree ).push_back( InvalidNodeUid );
//
//        auto t = f.open_transaction();
//        EXPECT_NO_THROW( save( tree, t ) );
//        EXPECT_EQ( RetCode::Ok, t.status() );
//        uid = tree.uid();
//        EXPECT_NE( InvalidNodeUid, uid );
//        EXPECT_EQ( RetCode::Ok, t.commit() );
//    }
//
//    {
//        StorageFile f( "serialization.jb", true );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTreeCache c( &f );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTree tree( uid, &f, &c );
//        EXPECT_EQ( 0, elements( tree ).size() );
//        EXPECT_EQ( 1, links( tree ).size() );
//        EXPECT_EQ( InvalidNodeUid, links( tree ).back() );
//    }
//
//    {
//        StorageFile f( "serialization.jb", true );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTreeCache c( &f );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTree tree( InvalidNodeUid, &f, &c );
//        elements( tree ) = etalon_elements;
//        links( tree ) = etalon_links;
//
//        auto t = f.open_transaction();
//        EXPECT_NO_THROW( save( tree, t ) );
//        EXPECT_EQ( RetCode::Ok, t.status() );
//        uid = tree.uid();
//        EXPECT_NE( InvalidNodeUid, uid );
//        EXPECT_EQ( RetCode::Ok, t.commit() );
//    }
//
//    {
//        StorageFile f( "serialization.jb", true );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTreeCache c( &f );
//        ASSERT_EQ( RetCode::Ok, f.status() );
//
//        BTree tree( uid, &f, &c );
//        EXPECT_EQ( etalon_elements, elements( tree ) );
//        EXPECT_EQ( etalon_links, links( tree ) );
//    }
//}


TYPED_TEST( TestBTree, Insert_Find )
{
    using namespace std;

    {
        // open starage
        StorageFile f( "Insert_Find_3.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // prepare cache
        BTreeCache c( &f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // get root node
        auto[ rc, root ] = c.get_node( RootNodeUid );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( root );

        // inserts 1000 elements into /root
        for ( Digest digest = 0; digest < 1000; ++digest )
        {
            // find place to insertion
            BTreePath bpath;
            auto[ rc, found ] = root->find_digest( digest, bpath );
            EXPECT_EQ( RetCode::Ok, rc );
            EXPECT_FALSE( found );

            {
                auto target = bpath.back(); bpath.pop_back();

                auto[ rc, node ] = c.get_node( target.first );
                EXPECT_EQ( RetCode::Ok, rc );

                EXPECT_EQ( RetCode::Ok, node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false ) );
            }
        }
    }

    {
        // open starage
        StorageFile f( "Insert_Find_3.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // prepare cache
        BTreeCache c( &f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // get root node
        auto[ rc, root ] = c.get_node( RootNodeUid );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( root );

        // collect b-tree leaf depths (balance check)
        std::set< size_t > depth;

        for ( Digest digest = 0; digest < 1000; ++digest )
        {
            // find the node
            BTreePath bpath;
            auto[ rc, found ] = root->find_digest( digest, bpath );
            EXPECT_EQ( RetCode::Ok, rc );
            EXPECT_TRUE( found );

            {
                auto[ rc, node ] = c.get_node( bpath.back().first );
                EXPECT_EQ( RetCode::Ok, rc );

                // validate value
                //EXPECT_EQ( Value{ to_string( digest ) }, node->value( bpath.back().second ) );

                // if node is leaf - insert depth into unique collection
                if ( is_leaf_element( *node, bpath.back().second ) )
                {
                    depth.insert( bpath.size() );
                }
            }
        }

        // check that tree is balanced
        EXPECT_GE( 2, depth.size() );
    }
}


TYPED_TEST( TestBTree, Insert_Ovewrite )
{
    using namespace std;

    // open starage
    StorageFile f( "Insert_Ovewrite.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // prepare cache
    BTreeCache c( &f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // get root node
    auto[ rc, root ] = c.get_node( RootNodeUid );
    EXPECT_EQ( RetCode::Ok, rc );
    EXPECT_TRUE( root );

    // inserts 10 elements into /root
    for ( Digest digest = 0; digest < 10; ++digest )
    {
        // find place to insertion
        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_FALSE( found );

        {
            auto target = bpath.back(); bpath.pop_back();

            auto[ rc, node ] = c.get_node( target.first );
            EXPECT_EQ( RetCode::Ok, rc );

            EXPECT_EQ( RetCode::Ok, node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false ) );
        }
    }

    // insert one more node with already present key
    {
        Digest digest = 7;

        // find place to insertion
        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( found );

        {
            auto target = bpath.back(); bpath.pop_back();

            auto[ rc, node ] = c.get_node( target.first );
            EXPECT_EQ( RetCode::Ok, rc );

            // try to insert
            EXPECT_EQ( RetCode::AlreadyExists, node->insert( target.second, bpath, digest, Value{ ( uint32_t )7 }, 0, false ) );
            EXPECT_EQ( RetCode::Ok, node->insert( target.second, bpath, digest, Value{ 7. }, 1, true ) );
        }
    }

    // find node and validate value & exiration mark
    {
        Digest digest = 7;

        // find the node
        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( found );

        {
            auto[ rc, node ] = c.get_node( bpath.back().first );
            EXPECT_EQ( RetCode::Ok, rc );

            // validate value and expiration time
            //EXPECT_EQ( Value{ 7. }, node->value( bpath.back().second ) );
            EXPECT_EQ( 1, node->good_before( bpath.back().second ) );

            // overwrite node without expiration mark
            auto target = bpath.back(); bpath.pop_back();
            EXPECT_EQ( RetCode::Ok, node->insert( target.second, bpath, digest, Value{ "Ok" }, 0, true ) );
        }
    }

    // find node and validate value & UNTOUCHED exiration mark
    {
        Digest digest = 7;

        // find the node
        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_TRUE( found );

        {
            auto[ rc, node ] = c.get_node( bpath.back().first );
            EXPECT_EQ( RetCode::Ok, rc );

            // validate value and expiration time
            //EXPECT_EQ( Value{ "Ok" }, node->value( bpath.back().second ) );
            EXPECT_EQ( 1, node->good_before( bpath.back().second ) );
        }
    }
}


TYPED_TEST( TestBTree, Insert_Erase )
{
    using namespace std;

    // open starage
    StorageFile f( "Insert_Erase.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // prepare cache
    BTreeCache c( &f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // get root node
    auto[ rc, root ] = c.get_node( RootNodeUid );
    EXPECT_EQ( RetCode::Ok, rc );
    EXPECT_TRUE( root );

    // inserts 1000 elements into /root
    for ( Digest digest = 0; digest < 1000; ++digest )
    {
        // find place to insertion
        BTreePath bpath;
        auto[ rc, found ] = root->find_digest( digest, bpath );
        EXPECT_EQ( RetCode::Ok, rc );
        EXPECT_FALSE( found );

        {
            auto target = bpath.back(); bpath.pop_back();

            auto[ rc, node ] = c.get_node( target.first );
            EXPECT_EQ( RetCode::Ok, rc );

            EXPECT_EQ( RetCode::Ok, node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false ) );
        }
    }

    // erase each 10th element
    for ( Digest digest = 0; digest < 1000; ++digest )
    {
        if ( digest % 10 == 0 )
        {
            // find element
            BTreePath bpath;
            auto[ rc, found ] = root->find_digest( digest, bpath );
            EXPECT_EQ( RetCode::Ok, rc );
            EXPECT_TRUE( found );

            {
                auto target = bpath.back(); bpath.pop_back();

                auto[ rc, node ] = c.get_node( target.first );
                EXPECT_EQ( RetCode::Ok, rc );

                EXPECT_EQ( RetCode::Ok, node->erase( target.second, bpath ) );
            }
        }
    }
}