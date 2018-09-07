#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <memory>
#include <tuple>
#include <algorithm>
#include <limits>
#include <exception>
#include <execution>
#include <iostream>

#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>

#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


class TestPackedValue;
template < typename T > class TestBTree;


namespace jb
{
    /** Implementstion of B-tree. Each key in the system holds their chilren keys as a B-tree.
    
    That allow us to minimize a number of file system access what is important for huge subkey
    numbers. B-tree insert, search, and erase operation have O( log(t,T) ) complexity, where t is
    B-tree factor known as minimum power

    @tparam Policies - global settings
    */
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl::BTree
    {
        friend class TestPackedValue;
        template < typename T > friend class TestBTree;

        friend class BTreeCache;
        friend class PhysicalVolumeImpl; // needs access to private save()

        //
        // few aliases
        //
        using Storage = Storage< Policies >;
        using Value = typename Storage::Value;
        using Digest = typename Bloom::Digest;
        using StorageFile = typename PhysicalVolumeImpl::StorageFile;
        using Transaction = typename StorageFile::Transaction;
        using BlobUid = typename StorageFile::ChunkUid;
        using big_uint64_t = boost::endian::big_uint64_at;

        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static_assert( BTreeMinPower >= 2, "B-tree power must be > 1" );
        static constexpr auto BTreeMin = BTreeMinPower - 1;
        static constexpr auto BTreeMax = 2 * BTreeMinPower - 1;
        static constexpr auto BTreeMaxDepth = Policies::PhysicalVolumePolicy::BTreeMaxDepth;

        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;


        //
        // represent element's value in packed form
        //
        struct PackedValue;
        friend std::ostream & operator << ( std::ostream & os, const PackedValue & v );
        friend std::istream & operator >> ( std::istream & is, PackedValue & v );


    public:

        //
        // more aliases
        //
        using BTreeP = std::shared_ptr< BTree >;
        using NodeUid = typename StorageFile::ChunkUid;
        static constexpr auto RootNodeUid = StorageFile::RootChunkUid;
        static constexpr auto InvalidNodeUid = StorageFile::InvalidChunkUid;


        /** Position inside b-tree node
        */
        using Pos = size_t;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };


        /** Represents a path inside a b-tree as a set of pairs b-tree node -> position
        */
        using BTreePath = static_vector< std::pair< NodeUid, Pos >, BTreeMaxDepth >;


        //struct BTreePath : public std::vector< std::pair< NodeUid, Pos > >
        //{
        //    BTreePath() { reserve(100); }
        //};


        /** Represents B-tree error
        */
        struct btree_error : public std::runtime_error
        {
            btree_error( RetCode rc, const char * what ) : std::runtime_error( what ), rc_( rc ) {}
            RetCode code() const { return rc_; }

        private:
            RetCode rc_;
        };


    private:

        /* Throws b-tree error if a condition failed

        @param [in] condition - condition to be checked
        @param [in] rc - return code to be assiged to an error
        @param [in] what - text message to be assigned to an error
        @throw btree_error
        */
        auto static throw_btree_error( bool condition, RetCode rc, const char * what = "" )
        {
            if ( !condition ) throw btree_error( rc, what );
        }


        /* Throws std::logic_error if a condition failed and immediately dies on noexcept guard

        calling terminate() handler. That gives the ability to collect crash dump for futher
        analysis

        @param [in] condition - condition to be checked
        @param [in] what - text message to be assigned to an error
        @throw nothing
        */
        auto static throw_logic_error( bool condition, const char * what = "" ) noexcept
        {
            if ( !condition ) throw std::logic_error( what );
        }


        //
        // represent b-tree element
        //
        struct Element
        {
            Digest digest_;
            uint64_t good_before_;
            NodeUid children_;
            PackedValue value_;

            //
            // provides LESSER relation for b-tree elements
            //
            friend bool operator < ( const Element & l, const Element & r ) noexcept
            {
                return l.digest_ < r.digest_;
            }
        };


        //
        // more aliases
        //
        using ElementCollection = static_vector< Element, BTreeMax  >;
        using LinkCollection = static_vector< NodeUid, BTreeMax + 1 >;

        //using ElementCollection = std::vector< Element  >;
        //using LinkCollection = std::vector< NodeUid >;

        //
        // data members
        //
        NodeUid uid_;
        StorageFile & file_;
        BTreeCache & cache_;
        mutable boost::upgrade_mutex guard_;
        ElementCollection elements_;
        LinkCollection links_;


        /* Output streaming operator for b-tree node element

        @param [in/out] os - output stream
        @param [in] e - element to be streamed out
        @retval std::ostream - updated output stream
        @throw btree_error, storage_file_error
        */
        friend std::ostream & operator << ( std::ostream & os, const Element & e )
        {
            big_uint64_t digest = e.digest_;
            big_uint64_t good_before = e.good_before_;
            big_uint64_t children = e.children_;
            big_uint64_t type_index = e.value_.type_index_;
            big_uint64_t value = e.value_.value_;

            os.write( reinterpret_cast< const char* >( &digest ), sizeof( digest ) );
            throw_btree_error( os.good(), RetCode::UnknownError );
            os.write( reinterpret_cast< const char* >( &good_before ), sizeof( good_before ) );
            throw_btree_error( os.good(), RetCode::UnknownError );
            os.write( reinterpret_cast< const char* >( &children ), sizeof( children ) );
            throw_btree_error( os.good(), RetCode::UnknownError );

            os << e.value_;

            return os;
        }


        /* Input streaming operator for b-tree node element

        @param [in/out] is - input stream
        @param [in] e - element to be streamed in
        @retval std::istream - updated input stream
        @throw btree_error, storage_file_error
        */
        friend std::istream & operator >> ( std::istream & is, Element & e )
        {
            big_uint64_t digest;
            big_uint64_t good_before;
            big_uint64_t children;

            is.read( reinterpret_cast< char* >( &digest ), sizeof( digest ) );
            throw_btree_error( is.good(), RetCode::UnknownError );
            is.read( reinterpret_cast< char* >( &good_before ), sizeof( good_before ) );
            throw_btree_error( is.good(), RetCode::UnknownError );
            is.read( reinterpret_cast< char* >( &children ), sizeof( children ) );
            throw_btree_error( is.good(), RetCode::UnknownError );

            e.digest_ = static_cast< Digest >( digest );
            e.good_before_ = good_before;
            e.children_ = children;

            is >> e.value_;

            return is;
        }


        /* Output streaming operator for b-tree node

        @param [in/out] os - output stream
        @param [in] node - b-tree node to be streamed out
        @retval std::ostream - updated output stream
        @throw btree_error, storage_file_error
        */
        friend std::ostream & operator << ( std::ostream & os, const BTree & node )
        {
            try
            {
                throw_logic_error( node.elements_.size() < BTreeMax, "Maximum size of b-tree node exceeded" );
                throw_logic_error( node.elements_.size() + 1 == node.links_.size(), "Broken b-tree node" );

                big_uint64_t size = node.elements_.size();
                os.write( reinterpret_cast< const char * >( &size ), sizeof( size ) );

                for ( const auto & e : node.elements_ )
                {
                    os << e;
                }

                for ( auto l : node.links_ )
                {
                    big_uint64_t link = l;
                    os.write( reinterpret_cast< const char * >( &link ), sizeof( link ) );
                }

                return os;
            }
            catch ( const std::logic_error & )
            {
                abort();
            }
        }


        /* Input streaming operator for b-tree node

        @param [in/out] is - input stream
        @param [in] node - node to be streamed in
        @retval std::istream - updated input stream
        @throw btree_error, storage_file_error
        */
        friend std::istream & operator >> ( std::istream & is, BTree & node )
        {
            big_uint64_t size;
            is.read( reinterpret_cast< char* >( &size ), sizeof( size ) );
            size_t sz = static_cast< size_t >( size );

            throw_btree_error( sz < BTreeMax, RetCode::InvalidData, "Maximum size of b-tree node exceeded" );

            node.elements_.resize( sz );
            node.links_.resize( sz + 1 );

            for ( auto & e : node.elements_ )
            {
                is >> e;
            }

            for ( auto & l : node.links_ )
            {
                big_uint64_t link;
                is.read( reinterpret_cast< char* >( &link ), sizeof( link ) );
                l = link;
            }

            return is;
        }


        /* Stores b-tree node to file

        @param [in] t - transaction
        @retval RetCode - operation status
        @throw btree_error, storage_file_error
        */
        void save( Transaction & t ) const
        {
            auto buffer = t.get_chain_writer< char >();

            std::ostream os( &buffer );
            os << *this;
            os.flush();

            NodeUid uid = t.get_first_written_chunk();
            std::swap( uid, const_cast< BTree* >( this )->uid_ );

            cache_.update_uid( uid, uid_ );
        }


        /* Stores b-tree node to file preserving node uid

        @param [in] t - transaction
        @throw btree_error, storage_file_error
        */
        void overwrite( Transaction & t ) const
        {
            auto buffer = t.get_chain_overwriter< char >( uid_ );
            
            std::ostream os( &buffer );
            os << *this;
            os.flush();
        }


        /* Loads b-tree node from a storage file

        @param [in] uid - uid of a node to be loaded
        @throw btree_error, storage_file_error
        */
        void load( NodeUid uid )
        {
            //auto buffer = file_.get_chain_reader< uint64_t >( uid );
            auto buffer = file_.get_chain_reader< char >( uid );
            
            std::istream is( &buffer );
            is >> *this;

            uid_ = uid;
        }


        /* Splits overflown node into 2 ones

        @param [in] l - the left part
        @param [in] r - the right part
        @throw nothing
        */
        auto split_overflown_node( BTree & l, BTree & r ) const noexcept
        {
            using namespace std;

            throw_logic_error( elements_.size() == BTreeMax, "A node is not overflown" );

            l.elements_.clear(); l.links_.clear();
            r.elements_.clear(); r.links_.clear();

            l.elements_.insert( begin( l.elements_ ), begin( elements_ ), begin( elements_ ) + BTreeMin );
            r.elements_.insert( begin( r.elements_ ), end( elements_ ) - BTreeMin, end( elements_ ) );

            l.links_.insert( begin( l.links_ ), begin( links_ ), begin( links_ ) + BTreeMin + 1 );
            r.links_.insert( begin( r.links_ ), end( links_ ) - BTreeMin - 1, end( links_ ) );
        }


        /* Insert new element into b-tree node

        @param [in] t - active transaction
        @param [in] pos - insert position
        @param [in] bpath - path to insert position
        @param [in] ow - if overwritting allowed
        @retval RetCode - operation status
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void insert_element( Transaction & t, Pos pos, BTreePath & bpath, const Element & e, bool ow )
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );
            throw_logic_error( elements_.size() < BTreeMax, "A node is overflown" );
            throw_logic_error( pos < elements_.size() + 1, "Invalid insert position" );

            // if the element exists
            if ( pos < elements_.size() && e.digest_ == elements_[ pos ].digest_ )
            {
                // if overwriting possible?
                throw_btree_error( ow, RetCode::AlreadyExists );

                // erase blob for old value
                elements_[ pos ].value_.erase_blob( t );

                // emplace element at existing position
                auto old_expiration = elements_[ pos ].good_before_;
                elements_[ pos ] = e;
                elements_[ pos ].good_before_ = elements_[ pos ].good_before_ ? elements_[ pos ].good_before_ : old_expiration;

                // overwrite node
                return overwrite( t );
            }
            else
            {
                // insert element at the pos
                assert( !pos || elements_[ pos - 1 ] < e && elements_.size() <= pos || e < elements_[ pos ] );
                elements_.insert( begin( elements_ ) + pos, e );
                links_.insert( begin( links_ ) + pos, InvalidNodeUid );
            }

            // if this node overflow
            if ( elements_.size() == BTreeMax )
            {
                bpath.empty() ? process_overflown_root( t ) : split_and_araise_median( t, bpath );
            }
            else
            {
                return overwrite( t );
            }
        }


        /* Processes overflow of the root node

        Splits root into 2 nodes and leaves only their mediane element in the root

        @param [out] - active transaction
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void process_overflown_root( Transaction & t )
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );
            throw_logic_error( elements_.size() == BTreeMax, "Root is not overflown" );

            // split this item into 2 new and save them
            BTree l( file_, cache_ ); BTree r( file_, cache_ );
            split_overflown_node( l, r );
            l.save( t );
            r.save( t );

            // leave only mediane element...
            elements_[ 0 ] = move( elements_[ BTreeMin ] );
            elements_.resize( 1 );

            // with links to new items
            links_[ 0 ] = l.uid(); links_[ 1 ] = r.uid();
            links_.resize( 2 );

            // ovewrite root node
            overwrite( t );
        }


        /* Process overflow of non root node

        Splits node into 2 ones and araises median element to the parent node

        @param [out] t - transaction
        @param [in] bpath - path in btree
        @retval RetCode - operation status
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void split_and_araise_median( Transaction & t, BTreePath & bpath )
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );
            throw_logic_error( elements_.size() == BTreeMax, "Root is not overflown" );
            throw_logic_error( bpath.size(), "This is root" );

            // split node into 2 new and save them
            BTree l( file_, cache_ ); BTree r( file_, cache_ );
            split_overflown_node( l, r );
            l.save( t );
            r.save( t );

            // get parent b-tree path
            BTreePath::value_type parent_path = move( bpath.back() ); bpath.pop_back();

            // remove this node from storage and drop it from cache
            t.erase_chain( uid_ );
            cache_.drop( uid_ );

            // and insert mediane element to parent
            auto parent = cache_.get_node( parent_path.first );

            parent->insert_araising_element(
                t,
                parent_path.second,
                bpath,
                l.uid_,
                move( elements_[ BTreeMin ] ),
                r.uid_
            );
        }


        /* Inserts araising element

        Just inserts element and check overflow condition

        @param [out] t - transaction
        @param [in] pos - insert position
        @param [in] bpath - path in b-tree
        @param [in] l_link - left link of araising element
        @param [in] e - element to be inserted
        @param [in] r_link - right link of the element
        @retval RetCode - operation status
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void insert_araising_element(
            Transaction & t,
            Pos pos,
            BTreePath & bpath,
            NodeUid l_link,
            const Element & e,
            NodeUid r_link )
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );

            // insert araising element
            elements_.insert( begin( elements_ ) + pos, e );
            links_.insert( begin( links_ ) + pos, l_link );
            links_[ pos + 1 ] = r_link;

            // check node for overflow
            if ( elements_.size() == BTreeMax )
            {
                // if this is root node of b-tree
                if ( bpath.empty() )
                {
                    process_overflown_root( t );
                }
                else
                {
                    split_and_araise_median( t, bpath );
                }
            }
            else
            {
                overwrite( t );
            }
        }


        /* Checks if b-tree node is a leaf node

        @retval bool - true if the node is a leaf
        @throw nothing
        */
        bool is_leaf() const noexcept
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );
            return find_if_not( begin( links_ ), end( links_ ), [] ( auto l ) { return InvalidNodeUid == l; } ) == end( links_ );
        }


        /* Erases an element at given position

        @param [in] transaction - active transaction
        @param [in] pos - position of an element to be erased
        @param [in] bpath - b-tree path
        @param [in] entry_level - level of a node in b-tree
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void erase_element( Transaction & t, Pos pos, BTreePath bpath, size_t entry_level )
        {
            using namespace std;

            throw_logic_error( pos < elements_.size(), "Invalid position" );
            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );

            // erase BLOB for the elements
            elements_[ pos ].value_.erase_blob( t );

            if ( !is_leaf() )
            {
                throw_logic_error( links_[ pos ] != InvalidNodeUid, "Imbalanced tree" );
                throw_logic_error( links_[ pos + 1 ] != InvalidNodeUid, "Imbalanced tree" );

                // get left child node
                BTreeP left_child = cache_.get_node( links_[ pos ] );

                // if left child is rich enough
                if ( left_child->elements_.size() > BTreeMin )
                {
                    auto bpath_size = bpath.size();

                    bpath.emplace_back( uid_, pos + 1 );
                    auto node = left_child;

                    // search for maximum element of the left subtree
                    while ( InvalidNodeUid != node->links_.back() )
                    {
                        bpath.emplace_back( node->uid_, node->elements_.size() );
                        node = cache_.get_node( node->links_.back() );
                    }

                    // bring the minimum element of right subtree
                    elements_[ pos ] = node->elements_.back();

                    // and erase it
                    node->erase_element( t, node->elements_.size() - 1, bpath, entry_level );

                    // update link to the right child
                    links_[ pos + 1 ] = left_child->uid_;

                    bpath.resize( bpath_size );

                    // save this one
                    entry_level == bpath.size() ? overwrite( t ) : save( t );

                    return;
                }

                // get right child node
                BTreeP right_child = cache_.get_node( links_[ pos + 1 ] );

                // if right child is rich enough
                if ( right_child->elements_.size() > BTreeMin )
                {
                    auto bpath_size = bpath.size();

                    bpath.emplace_back( uid_, pos + 1 );
                    auto node = right_child;

                    // search for minimum element of the right subtree
                    while ( InvalidNodeUid != node->links_[ 0 ] )
                    {
                        bpath.emplace_back( node->uid_, 0 );
                        node = cache_.get_node( node->links_[ 0 ] );
                    }

                    // bring the minimum element of the right subtree
                    elements_[ pos ] = node->elements_[ 0 ];

                    // and erase it
                    node->erase_element( t, 0, bpath, entry_level );

                    // update link to the right child
                    links_[ pos + 1 ] = right_child->uid_;

                    bpath.resize( bpath_size );

                    // save this one
                    entry_level == bpath.size() ? overwrite( t ) : save( t );

                    return;
                }

                // left child absorbs this element and right child
                left_child->absorb( elements_[ pos ], *right_child );
                elements_.erase( begin( elements_ ) + pos );
                links_.erase( begin( links_ ) + pos + 1 );

                if ( elements_.size() )
                {
                    bpath.emplace_back( uid_, pos );
                    left_child->erase_element( t, BTreeMin, bpath, entry_level );
                    bpath.pop_back();

                    links_[ pos ] = left_child->uid_;

                    t.erase_chain( right_child->uid_ );
                    cache_.drop( right_child->uid_ );
                }
                else
                {
                    elements_ = left_child->elements_;
                    links_ = left_child->links_;

                    erase_element( t, BTreeMin, bpath, entry_level );

                    t.erase_chain( left_child->uid_ );
                    cache_.drop( left_child->uid_ );

                    t.erase_chain( right_child->uid_ );
                    cache_.drop( right_child->uid_ );
                }

                // save this one
                entry_level == bpath.size() ? overwrite( t ) : save( t );
            }
            else
            {
                elements_.erase( begin( elements_ ) + pos );
                links_.erase( begin( links_ ) + pos );

                if ( elements_.size() < BTreeMin && uid_ != RootNodeUid )
                {
                    return process_leaf_underflow( t, bpath, entry_level );
                }
                else
                {
                    // save this one
                    entry_level < bpath.size() ? save( t ) : overwrite( t );
                }
            }
        }


        /* Handles underflown leaf upon erasing operation

        @param [in] transaction - active transaction
        @param [in] bpath - b-tree path
        @param [in] entry_level - level of a node containing element to be erased
        @throw btree_error, btree_cache_error, storage_file_error
        */
        void process_leaf_underflow( Transaction & t, BTreePath & bpath, size_t entry_level )
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );
            throw_logic_error( elements_.size() < BTreeMin, "The node is not underflown" );
            throw_logic_error( is_leaf(), "The node is not a leaf" );

            // if this is the root node
            if ( bpath.empty() )
            {
                overwrite( t );
                return;
            }

            // exract parent info from the path
            auto parent_ref = move( bpath.back() );

            // get parent node
            auto parent = cache_.get_node( parent_ref.first );

            // get left sibling node
            BTreeP left_sibling;

            // if left sibling exists - load it
            if ( 0 < parent_ref.second && InvalidNodeUid != parent->links_[ parent_ref.second - 1 ] )
            {
                left_sibling = cache_.get_node( parent->links_[ parent_ref.second - 1 ] );
                throw_logic_error( left_sibling->is_leaf(), "Imbalanced tree" );
            }

            // if left sibling is rich
            if ( left_sibling && left_sibling->elements_.size() > BTreeMin )
            {
                // prepend parent key to this node
                elements_.insert( begin( elements_ ), parent->elements_[ parent_ref.second - 1 ] );
                links_.push_back( InvalidNodeUid );

                // set parent key to the last of left sibling
                parent->elements_[ parent_ref.second - 1 ] = left_sibling->elements_.back();

                // consume the last of left sibling
                auto e = left_sibling->elements_.back().digest_;
                left_sibling->elements_.pop_back();
                left_sibling->links_.pop_back();

                // save this and the sibling
                save( t );
                left_sibling->save( t );

                // update parent's links
                parent->links_[ parent_ref.second ] = uid_;
                parent->links_[ parent_ref.second - 1 ] = left_sibling->uid_;

                // done: save parent 
                if ( entry_level == bpath.size() )
                {
                    parent->overwrite( t );
                }

                return;
            }

            // get right sibling node
            BTreeP right_sibling;

            if ( parent_ref.second + 1 < parent->elements_.size() && InvalidNodeUid != parent->links_[ parent_ref.second + 1 ] )
            {
                right_sibling = cache_.get_node( parent->links_[ parent_ref.second + 1 ] );
                throw_logic_error( right_sibling->is_leaf(), "Imbalanced tree" );
            }

            // if right sibling is rich
            if ( right_sibling && right_sibling->elements_.size() > BTreeMin )
            {
                // append parent key
                elements_.push_back( parent->elements_[ parent_ref.second + 1 ] );
                links_.push_back( InvalidNodeUid );

                // assign parent key with the first of right sibling
                parent->elements_[ parent_ref.second + 1 ] = right_sibling->elements_.front();

                // consume the first element of right sibling
                auto e = right_sibling->elements_[ 0 ].digest_;
                right_sibling->elements_.erase( begin( right_sibling->elements_ ) );
                right_sibling->links_.pop_back();

                // save this and the sibling
                save( t );
                right_sibling->save( t );

                // update parent's links
                parent->links_[ parent_ref.second ] = uid_;
                parent->links_[ parent_ref.second + 1 ] = right_sibling->uid_;

                // done: save parent 
                if ( entry_level == bpath.size() )
                {
                    parent->overwrite( t );
                }

                return;
            }

            // if both sibling are poor and left sibling exists
            if ( left_sibling )
            {
                // merge this b-tree node with the left sibling
                left_sibling->absorb( parent->elements_[ parent_ref.second - 1 ], *this );

                parent->elements_.erase( begin( parent->elements_ ) + parent_ref.second - 1 );
                parent->links_.erase( begin( parent->links_ ) + parent_ref.second );

                t.erase_chain( uid_ );
                cache_.drop( uid_ );

                if ( parent->elements_.size() )
                {
                    left_sibling->save( t );
                    parent->links_[ parent_ref.second - 1 ] = left_sibling->uid_;
                }
                else
                {
                    parent->elements_ = left_sibling->elements_;
                    parent->links_ = left_sibling->links_;

                    t.erase_chain( left_sibling->uid_ );
                    cache_.drop( left_sibling->uid_ );
                }

                // done: save parent 
                if ( entry_level == bpath.size() )
                {
                    parent->overwrite( t );
                }

                return;
            }

            // if both siblings are poor and right sibling exists
            if ( right_sibling )
            {
                // merge this b-tree node with the left sibling
                absorb( parent->elements_[ parent_ref.second ], *right_sibling );

                parent->elements_.erase( begin( parent->elements_ ) + parent_ref.second );
                parent->links_.erase( begin( parent->links_ ) + parent_ref.second + 1 );

                t.erase_chain( right_sibling->uid_ );
                cache_.drop( right_sibling->uid_ );

                if ( parent->elements_.size() )
                {
                    save( t );
                    parent->links_[ parent_ref.second ] = uid_;
                }
                else
                {
                    parent->elements_ = elements_;
                    parent->links_ = links_;

                    t.erase_chain( uid_ );
                    cache_.drop( uid_ );
                }

                // done: save parent 
                if ( entry_level == bpath.size() )
                {
                    parent->overwrite( t );
                }

                return;
            }

            // die
            throw_logic_error( false, "Imbalanced b-tree" );
        }


        /* Absorbs element and right sibling node. Given element becomes new mediane

        @param [in] mediane - element to be used as new mediane
        @param [in] right_sibling - right sibling node
        @throw nothing
        */
        void absorb( const Element & mediane, BTree & right_sibling ) noexcept
        {
            using namespace std;

            throw_logic_error( elements_.size() + right_sibling.elements_.size() + 1 <= BTreeMax );

            elements_.insert( end( elements_ ), mediane );
            elements_.insert( end( elements_ ), begin( right_sibling.elements_ ), end( right_sibling.elements_ ) );
            links_.insert( end( links_ ), begin( right_sibling.links_ ), end( right_sibling.links_ ) );
        }


    public:

        /** The class is not default creatible/copyable/movable
        */
        BTree() = delete;
        BTree( BTree&& ) = delete;


        /* Explicit constructor

        @param [in] file - associated storage file
        @param [in] cache - associated b-tree node cache
        @throw nothing
        */
        explicit BTree( StorageFile & file, BTreeCache & cache ) noexcept
            : uid_( InvalidNodeUid )
            , file_( file )
            , cache_( cache )
        {
            links_.push_back( InvalidNodeUid );
        }


        /** Provides b-tree node uid

        @retval NodeUid - uid
        @throw nothing
        */
        auto uid() const noexcept { return uid_; }


        /** Provides access to b-tree node guard

        @retval boost::upgrade_mutex - guard
        @throw nothing
        */
        auto & guard() const noexcept { return guard_; }


        /** Provides value of an element at given position

        @param [in] ndx - position
        @retval Value - the element's value
        @throw btree_error, storage_file_error
        */
        const auto value( size_t ndx )
        {
            throw_logic_error( ndx < elements_.size(), "Invalid position" );
            return elements_[ ndx ].value_.unpack( file_ );
        }


        /** Provides expiration timemark for an element at given position

        @param [in] ndx - element position
        @retval uint64_t - expiration time in msecs from epoch
        */
        const auto & good_before( size_t ndx ) const noexcept
        {
            throw_logic_error( ndx < elements_.size(), "Invalid position" );
            return elements_[ ndx ].good_before_;
        }


        /** Provides UID of b-tree containing element's children

        @param [in] ndx - element position
        @retval NodeUid - uid of children b-tree
        @throw nothing
        */
        const auto & children( size_t ndx ) const noexcept
        {
            throw_logic_error( ndx < elements_.size(), "Invalid position" );
            return elements_[ ndx ].children_;
        }


        /** Searches through b-tree for given key digest...

        accumulates search path that can be later used as a hint for upcoming operation

        @param [in] digest - key to be found
        @param [out] path - search path
        @retval bool - if digest found
        @throw btree_error, btree_cache_error, storage_file_error
        */
        bool find_digest( Digest digest, BTreePath & path ) const
        {
            using namespace std;

            throw_logic_error( elements_.size() + 1 == links_.size(), "Broken b-tree node" );

            Element e{ digest };
            auto lower = lower_bound( begin( elements_ ), end( elements_ ), e );

            size_t d = static_cast< size_t >( std::distance( begin( elements_ ), lower ) );
            path.emplace_back( uid_, d );

            auto link = InvalidNodeUid;

            if ( lower != end( elements_ ) )
            {
                if ( lower->digest_ == e.digest_ )
                {
                    return true;
                }
                else
                {
                    link = links_[ d ];
                }
            }
            else
            {
                link = links_.back();
            }

            if ( link == InvalidNodeUid )
            {
                return false;
            }
            else
            {
                throw_btree_error( path.size() < path.capacity(), RetCode::SubkeyLimitReached );

                auto child = cache_.get_node( link );
                return child->find_digest( digest, path );
            }
        }


        /** Inserts new subkey with given parameters to the key at specified position

        @param [in] pos - insert position
        @param [in] bpath - path from b-tree root
        @param [in] digest - subkey digest
        @param [in] value - value to be assigned to new subkey
        @param [in] good_before - expiration mark for the subkey
        @param [in] overwrite - overwrite existing subkey
        @throw btree_error, btree_cache_error, storage_file_error
        */
        auto insert( Pos pos, BTreePath & bpath, Digest digest, const Value & value, uint64_t good_before, bool overwrite )
        {
            throw_logic_error( pos <= elements_.size(), "Invalid position" );

            // open transaction
            auto t = file_.open_transaction();

            // pack value
            PackedValue p = PackedValue::make_packed( t, value );

            // insert element
            Element e{ digest, good_before, InvalidNodeUid, p };
            insert_element( t, pos, bpath, e, overwrite );

            // finalize transaction
            t.commit();
        }


        /** Erases specified b-tree node element

        @param [in] pos - position of element to be removed
        @param [in] bpath - path from b-tree root
        @throw btree_error, btree_cache_error, storage_file_error
        */
        auto erase( Pos pos, BTreePath & bpath )
        {
            throw_logic_error( pos < elements_.size(), "Invalid position" );

            // open transaction
            auto t = file_.open_transaction();

            // erase children b-tree if empty
            if ( InvalidNodeUid != elements_[ pos ].children_ )
            {
                auto children = cache_.get_node( elements_[ pos ].children_ );
                throw_btree_error( !children->elements_.size(), RetCode::NotLeaf );

                t.erase_chain( elements_[ pos ].children_ );
            }

            // erase the element
            erase_element( t, pos, bpath, bpath.size() );

            // finalize transaction
            t.commit();
        }


        /** Deploy children b-tree for specified element

        @param [in] pos - element position
        @throw btree_error, btree_cache_error, storage_file_error
        */
        auto deploy_children_btree( Pos pos )
        {
            throw_logic_error( pos < elements_.size(), "Invalid position" );

            if ( InvalidNodeUid == elements_[ pos ].children_ )
            {
                auto t = file_.open_transaction();

                BTree children( file_, cache_ );
                children.save( t );

                elements_[ pos ].children_ = children.uid_;
                overwrite( t );

                t.commit();
            }
        }
    };
}


#include "packed_value.h"


#endif