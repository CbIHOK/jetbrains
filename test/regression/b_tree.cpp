#include <gtest/gtest.h>
#include <storage.h>
#include "policies.h"


struct BTreePower_16 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 16;
    };

    static constexpr size_t BTreeSize = 10000;
};


struct BTreePower_32 : public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 32;
    };
    static constexpr size_t BTreeSize = 10000;
};


struct BTreePower_64: public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 64;
    };

    static constexpr size_t BTreeSize = 10000;
};


struct BTreePower_128: public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 128;
    };

    static constexpr size_t BTreeSize = 10000;
};


struct BTreePower_Default: public ::jb::DefaultPolicy<>
{
    struct PhysicalVolumePolicy : public ::jb::DefaultPolicy<>::PhysicalVolumePolicy
    {
        static constexpr size_t BloomSize = 1024;
        static constexpr size_t BTreeMinPower = 256;
    };

    static constexpr size_t BTreeSize = 10000;
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
    using BTreeP = typename BTree::BTreeP;
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
    using btree_error = typename BTree::btree_error;

    static constexpr auto RootNodeUid = BTree::RootNodeUid;
    static constexpr auto InvalidNodeUid = BTree::InvalidNodeUid;
    static constexpr size_t BTreeSize = Policy::BTreeSize;


    static auto & elements( BTree & t ) noexcept { return t.elements_; }
    static auto & links( BTree & t ) noexcept { return t.links_; }
    static auto save( BTree & tree, Transaction & t ) { return tree.save( t ); }

    void deploy_root( StorageFile & f, BTreeCache & c )
    {
        BTreeP root = std::make_shared< BTree >( f, c );
        auto t = f.open_transaction();
        root->save( t );
        t.commit();
    }

    bool is_leaf( BTree & node )
    {
        return node.is_leaf();
    }

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


typedef ::testing::Types< 
    BTreePower_16, 
    BTreePower_32,
    BTreePower_64,
    BTreePower_128,
    BTreePower_Default
> TestingPolicies;

TYPED_TEST_CASE( TestBTree, TestingPolicies );



TYPED_TEST( TestBTree, Insert_Find_CheckValue )
{
    using namespace std;

    {
        // open starage
        StorageFile f( "Insert_Find.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );
        ASSERT_TRUE( f.newly_created() );

        // prepare cache
        BTreeCache c( f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // deploy root node
        ASSERT_NO_THROW( deploy_root( f, c ) );

        EXPECT_NO_THROW(

        auto root = c.get_node( RootNodeUid );
        EXPECT_TRUE( root );

        // inserts 1000 elements into /root
        for ( Digest digest = 0; digest < BTreeSize; ++digest )
        {
            // find place to insertion
            BTreePath bpath;
            auto found = root->find_digest( digest, bpath );
            EXPECT_FALSE( found );

            auto target = bpath.back(); bpath.pop_back();
            auto node = c.get_node( target.first );
            EXPECT_TRUE( node );

            node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false );
        }

        );
    }

    {
        // open starage
        StorageFile f( "Insert_Find.jb", true );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // prepare cache
        BTreeCache c( f );
        ASSERT_EQ( RetCode::Ok, f.status() );

        // deploy root node
        ASSERT_NO_THROW( deploy_root( f, c ) );

        EXPECT_NO_THROW(

        auto root = c.get_node( RootNodeUid );
        EXPECT_TRUE( root );

        // collect b-tree leaf depths (balance check)
        std::set< size_t > depth;

        for ( Digest digest = 0; digest < BTreeSize; ++digest )
        {
            BTreePath bpath;

            auto found = root->find_digest( digest, bpath );
            EXPECT_TRUE( found );

            auto node = c.get_node( bpath.back().first );
            EXPECT_TRUE( node );

            Value v = node->value( bpath.back().second );
            Value etalon{ to_string( digest ) };
            EXPECT_EQ( etalon, v );

            // if node is leaf - insert depth into unique collection
            if ( is_leaf( *node ) )
            {
                depth.insert( bpath.size() );
            }
        }

        // check that tree is balanced
        EXPECT_GE( 2, depth.size() );

        );
    }
}


TYPED_TEST( TestBTree, Insert_Ovewrite )
{
    using namespace std;

    // open starage
    StorageFile f( "Insert_Ovewrite.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // prepare cache
    BTreeCache c( f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // deploy root node
    ASSERT_NO_THROW( deploy_root( f, c ) );

    // get root node
    BTreeP root;
    EXPECT_NO_THROW( root = c.get_node( RootNodeUid ) );
    EXPECT_TRUE( root );

    // inserts 10 elements into /root
    for ( Digest digest = 0; digest < 10; ++digest )
    {
        //EXPECT_NO_THROW(

        BTreePath bpath;
        auto found = root->find_digest( digest, bpath );
        EXPECT_FALSE( found );

        auto target = bpath.back(); bpath.pop_back();
        auto node = c.get_node( target.first );
        EXPECT_TRUE( node );

        node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false );

        //);
    }

    // insert one more node with already present key
    {
        Digest digest = 7;

        //EXPECT_NO_THROW(
        
        BTreePath bpath;
        auto found = root->find_digest( digest, bpath );
        EXPECT_TRUE( found );

        auto target = bpath.back(); bpath.pop_back();
        auto node = c.get_node( target.first );
        EXPECT_TRUE( node );

        // try to insert
        EXPECT_THROW( node->insert( target.second, bpath, digest, Value{ ( uint32_t )7 }, 0, false ), btree_error );
        node->insert( target.second, bpath, digest, Value{ 7. }, 1, true );
        
        //);
    }

    // find node and validate value & exiration mark
    {
        Digest digest = 7;

        //EXPECT_NO_THROW(

        BTreePath bpath;
        auto found = root->find_digest( digest, bpath );
        EXPECT_TRUE( found );

        auto node = c.get_node( bpath.back().first );
        EXPECT_TRUE( node );

        // validate value and expiration time
        //EXPECT_EQ( Value{ 7. }, node->value( bpath.back().second ) );
        EXPECT_EQ( 1, node->good_before( bpath.back().second ) );

        // overwrite node without expiration mark
        auto target = bpath.back(); bpath.pop_back();
        node->insert( target.second, bpath, digest, Value{ "Ok" }, 0, true );

        //);
    }

    EXPECT_NO_THROW(

    // find node and validate value & UNTOUCHED exiration mark
    {
        Digest digest = 7;

        BTreePath bpath;
        auto found = root->find_digest( digest, bpath );
        EXPECT_TRUE( found );

        auto node = c.get_node( bpath.back().first );
        EXPECT_TRUE( node );

        // validate value and expiration time
        //EXPECT_EQ( Value{ "Ok" }, node->value( bpath.back().second ) );
        EXPECT_EQ( 1, node->good_before( bpath.back().second ) );

    }

    );
}


TYPED_TEST( TestBTree, Insert_Erase )
{
    using namespace std;

    // open starage
    StorageFile f( "Insert_Erase.jb", true );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // prepare cache
    BTreeCache c( f );
    ASSERT_EQ( RetCode::Ok, f.status() );

    // deploy root node
    ASSERT_NO_THROW( deploy_root( f, c ) );

    // get root node
    BTreeP root;
    EXPECT_NO_THROW( root = c.get_node( RootNodeUid ) );
    EXPECT_TRUE( root );

    EXPECT_NO_THROW(

    // inserts 1000 elements into /root
    for ( Digest digest = 0; digest < BTreeSize; ++digest )
    {
        BTreePath bpath;

        auto found = root->find_digest( digest, bpath );
        EXPECT_FALSE( found );

        auto target = bpath.back(); bpath.pop_back();
        auto node = c.get_node( target.first );
        node->insert( target.second, bpath, digest, Value{ to_string( digest ) }, 0, false );
        {
            auto found = root->find_digest( digest, bpath );
            EXPECT_TRUE( found );
        }
    }

    BTreePath bpath;
    auto found = root->find_digest( 900, bpath );
    EXPECT_TRUE( found );

    );

    EXPECT_NO_THROW(
        
    // erase each 10th element
    for ( Digest digest = 0; digest < BTreeSize; ++digest )
    {
        if ( digest % 10 == 0 )
        {
            BTreePath bpath;
            auto found = root->find_digest( digest, bpath );
            EXPECT_TRUE( found );

            auto target = bpath.back(); bpath.pop_back();
            auto node = c.get_node( target.first );
            EXPECT_TRUE( node );

            node->erase( target.second, bpath );
        }
    }

    );

    EXPECT_NO_THROW(

    // collect b-tree leaf depths (balance check)
    std::set< size_t > depth;

    // inserts 1000 elements into /root
    for ( Digest digest = 0; digest < BTreeSize; ++digest )
    {
        if ( digest % 10 == 0 )
        {
            BTreePath bpath;
            auto found = root->find_digest( digest, bpath );
            EXPECT_FALSE( found );
        }
        else
        {
            BTreePath bpath;
            auto found = root->find_digest( digest, bpath );
            EXPECT_TRUE( found );

            auto target = bpath.back(); bpath.pop_back();
            auto node = c.get_node( target.first );

            // if node is leaf - insert depth into unique collection
            if ( is_leaf( *node ) )
            {
                depth.insert( bpath.size() );
            }
        }
    }

    // check that tree is balanced
    EXPECT_GE( 2, depth.size() );

    );
}