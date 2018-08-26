#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <memory>
#include <tuple>
#include <algorithm>
#include <limits>
#include <exception>
#include <execution>

#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/serialization/split_member.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/collection_size_type.hpp>
#include <boost/archive/binary_oarchive.hpp>


class TestBTree;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::BTree
    {
        friend class TestBTree;
        friend class boost::serialization::access;

        using Storage = Storage< Policies, Pad >;
        using Value = typename Storage::Value;
        using Digest = typename Bloom::Digest;
        using Transaction = typename StorageFile::Transaction;

        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static_assert( BTreeMinPower >= 2, "B-tree power must be > 1" );
        static constexpr auto BTreeMin = BTreeMinPower - 1;
        static constexpr auto BTreeMax = 2 * BTreeMinPower - 1;

        static constexpr auto BTreeMaxDepth = Policies::PhysicalVolumePolicy::BTreeMaxDepth;

        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

    public:

        using BTreeP = std::shared_ptr< BTree >;
        using NodeUid = typename StorageFile::ChunkUid;

        static constexpr auto RootNodeUid = StorageFile::RootChunkUid;
        static constexpr auto InvalidNodeUid = StorageFile::InvalidChunkUid;

        using Pos = size_t;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };

        using BTreePath = static_vector< std::pair< NodeUid, Pos >, BTreeMaxDepth >;


    private:


        //
        // represent b-tree element
        //
        struct Element
        {
            friend class boost::serialization::access;

            Digest digest_;
            Value value_;
            uint64_t good_before_;
            NodeUid children_;


            //
            // serializes b-tree element
            //
            template < class Archive >
            void save( Archive & ar, const unsigned int version ) const
            {
                ar << BOOST_SERIALIZATION_NVP( digest_ );
                ar << BOOST_SERIALIZATION_NVP( good_before_ );
                ar << BOOST_SERIALIZATION_NVP( children_ );
                
                size_t var_index = value_.index();
                ar << BOOST_SERIALIZATION_NVP( var_index );

                std::visit( [&] ( const auto & v ) {
                    ar << BOOST_SERIALIZATION_NVP( v );
                }, value_ );
            }


            //
            // deserializes element's value bsed on index
            //
            template < size_t I, class Archive >
            Value try_deserialize_variant( size_t index, Archive & ar, const unsigned int version )
            {
                if constexpr ( I < std::variant_size_v< Value > )
                {
                    if ( index == I )
                    {
                        std::variant_alternative_t< I, Value > v;
                        ar >> BOOST_SERIALIZATION_NVP( v );
                        return Value( v );
                    }
                    else
                    {
                        return try_deserialize_variant< I + 1 >( index, ar, version );
                    }
                }
                else
                {
                    throw std::runtime_error( "Unable to deserialize variant object" );
                }
            }


            //
            // deserializes b-tree element
            //
            template < class Archive >
            void load( Archive & ar, const unsigned int version )
            {
                ar >> BOOST_SERIALIZATION_NVP( digest_ );
                ar >> BOOST_SERIALIZATION_NVP( good_before_ );
                ar >> BOOST_SERIALIZATION_NVP( children_ );

                size_t var_index;
                ar >> BOOST_SERIALIZATION_NVP( var_index );
                value_ = std::move( try_deserialize_variant< 0 >( var_index, ar, version ) );
            }
            BOOST_SERIALIZATION_SPLIT_MEMBER()


            //
            // variant comparer
            // 
            struct var_cmp
            {
                bool value = true;

                template < typename U, typename V >
                void operator () ( const U &, const V & ) { value = false; }

                template < typename T >
                void operator () ( const T & l, const T & r ) { value = ( l == r ); }
            };

            //
            // compare b-tree element, need only for UT
            //
            friend bool operator == ( const Element & l, const Element & r )
            {
                if ( l.digest_ != r.digest_ || l.good_before_ != r.good_before_ || l.children_ != r.children_ ) return false;
                var_cmp cmp;
                std::visit( cmp, l.value_, r.value_ );
                return cmp.value;
            }


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
        using ElementCollection = static_vector< Element, BTreeMax + 1 >;
        using LinkCollection = static_vector< NodeUid, BTreeMax + 2 >;


        //
        // data members
        //
        NodeUid uid_;
        StorageFile * file_ = nullptr;
        BTreeCache * cache_ = nullptr;
        mutable boost::upgrade_mutex guard_;
        ElementCollection elements_;
        LinkCollection links_;


        //
        // serialization
        //
        template<class Archive>
        void save( Archive & ar, const unsigned int version ) const
        {
            using namespace boost::serialization;

            if ( elements_.size() > BTreeMax || elements_.size() + 1 != links_.size() ) throw std::logic_error( "Broken b-tree" );

            const collection_size_type element_count( elements_.size() );
            ar << BOOST_SERIALIZATION_NVP( element_count );

            if ( !elements_.empty() )
            {
                ar << make_array< const Element, collection_size_type >( 
                    static_cast< const Element* >( &elements_[ 0 ] ),
                    element_count
                    );
            }

            const collection_size_type link_count( links_.size() );
            ar << BOOST_SERIALIZATION_NVP( link_count );

            if ( !links_.empty() )
            {
                ar << make_array< const NodeUid, collection_size_type >(
                    static_cast< const NodeUid* >( &links_[ 0 ] ),
                    link_count
                    );
            }
        }


        //
        // deserialization
        //
        template<class Archive>
        void load( Archive & ar, const unsigned int version )
        {
            using namespace boost::serialization;

            collection_size_type element_count;
            ar >> BOOST_SERIALIZATION_NVP( element_count );

            if ( element_count > BTreeMax ) throw std::runtime_error( "Maximum number of elements exceeded" );

            elements_.resize( element_count );

            if ( !elements_.empty() )
            {
                ar >> make_array< Element, collection_size_type >(
                    static_cast< Element* >( &elements_[ 0 ] ),
                    element_count
                    );
            }
            
            collection_size_type link_count;
            ar >> BOOST_SERIALIZATION_NVP( link_count );

            if ( link_count > BTreeMax + 1 ) throw std::runtime_error( "Maximum number of links exceeded" );

            links_.resize( link_count );
            {
                ar >> make_array< NodeUid, collection_size_type >(
                    static_cast< NodeUid* >( &links_[ 0 ] ),
                    link_count
                    );
            }

            if ( elements_.size() + 1 != links_.size() ) throw std::runtime_error( "Broken b-tree" );
        }
        BOOST_SERIALIZATION_SPLIT_MEMBER()


        /* Stores b-tree node to file

        @param [in] t - transaction
        @retval RetCode - operation status
        @throw may throw std::exception for different reasons
        */
        [[nodiscard]]
        auto save( Transaction & t ) const
        {
            if ( !file_ || !cache_ )
            {
                return RetCode::UnknownError;
            }

            auto osbuf = t.get_chain_writer();
            std::ostream os( &osbuf );
            boost::archive::binary_oarchive ar( os );
            ar & *this;
            os.flush();

            if ( RetCode::Ok != t.status() )
            {
                return t.status();
            }

            NodeUid uid = t.get_first_written_chunk();
            std::swap( uid, const_cast< BTree* >( this )->uid_ );
            
            return cache_->update_uid( uid, uid_ );
        }


        /* Stores b-tree node to file preserving node uid

        @param [in] t - transaction
        @throw may throw std::exception for different reasons
        */
        [[nodiscard]]
        auto overwrite( Transaction & t ) const
        {
            if ( !file_ )
            {
                return RetCode::UnknownError;
            }

            auto osbuf = t.get_chain_overwriter( uid_ );
            std::ostream os( &osbuf );
            boost::archive::binary_oarchive ar( os );
            ar & *this;
            os.flush();

            return t.status();
        }


        /* Inserts new element into b-tree

        @param [in] e - element to be inserted
        @param [in] overwrite - if overwriting allowed
        @retval RetCode - operation status
        @throw nothing
        */
        [[nodiscard]]
        auto insert_element( Transaction & t, Element && e, bool overwrite = false ) noexcept
        {
            using namespace std;

            try
            {
                BTreePath bpath;

                // find target b-tree node
                if ( auto[ rc, node, pos ] = find_digest( e.digest_, bpath ); RetCode::Ok != rc )
                {
                    return rc;
                }
                else
                {
                    // and insert elelement
                    return insert_element( t, bpath, pos, move( e ), overwrite );
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        /* Splits node in parallel

        @param [in] l - the left part
        @param [in] r - the right part
        @throw may throw std::exception
        */
        auto split_node( BTreeP l, BTreeP r )
        {
            using namespace std;

            assert( l && r );
            assert( elements_.size() > BTreeMax );

            // resizing
            l->elements_.resize( BTreeMin ); l->links_.resize( BTreeMin + 1 );
            r->elements_.resize( BTreeMin ); r->links_.resize( BTreeMin + 1 );

            auto futures = array< std::future< void >, 4>{
                // copy first BTreeMin elements to the left
                async( launch::async, [&] { copy( execution::par, begin( elements_ ), begin( elements_ ) + BTreeMin, begin( l->elements_ ) ); } ),

                    // copy last BTreeMin elements to the right
                    async( launch::async, [&] { copy( execution::par, end( elements_ ) - BTreeMin, end( elements_ ), begin( r->elements_ ) ); } ),

                    // copy first BTreeMin + 1 links to the left
                    async( launch::async, [&] { copy( execution::par, begin( links_ ), begin( links_ ) + BTreeMin + 1, begin( l->links_ ) ); } ),

                    // copy last BTreeMin + 1 links to the right
                    async( launch::async, [&] { copy( execution::par, end( links_ ) - BTreeMin - 1, end( links_ ), begin( r->links_ ) ); } ),
            };

            // synchronization
            for ( const auto & f : futures ) { f.wait(); }
        }


        /* Insert new element into b-tree node

        @param [in] t - transaction
        @param [in] bpath - path to insert position
        @param [in] pos - insert position
        @param [in] ow - if overwritting allowed
        @retval RetCode - operation status
        @throw nothing
        */
        [[nodiscard]]
        auto insert_element( Transaction & t, BTreePath & bpath, Pos pos, Element && e, bool ow ) noexcept
        {
            using namespace std;

            try
            {
                assert( pos < elements_.size() + 1 );
                assert( elements_.size() <= BTreeMax );

                // if the element exists
                if ( pos < elements_.size() && e.digest_ == elements_[ pos ].digest_ )
                {
                    // if overwriting possible
                    if ( ow )
                    {
                        // emplace element at existing position
                        auto old_expiration = elements_[ pos ].good_before_;
                        elements_[ pos ] = move( e );
                        elements_[ pos ].good_before_ = elements_[ pos ].good_before_ ? elements_[ pos ].good_before_ : old_expiration;

                        // overwrite node
                        return overwrite( t );
                    }
                    else
                    {
                        return RetCode::AlreadyExists;
                    }
                }
                else
                {
                    // insert element at the pos
                    assert( !pos || elements_[ pos - 1 ] < e && elements_.size() <= pos || e < elements_[ pos ] );
                    elements_.emplace( begin( elements_ ) + pos, move( e ) );
                    links_.insert( begin( links_ ) + pos, InvalidNodeUid );
                }

                // if this node overflow
                if ( elements_.size() > BTreeMax )
                {
                    // if this is root node of b-tree
                    if ( bpath.empty() )
                    {
                        return process_overflow_root( t );
                    }
                    else
                    {
                        return split_and_araise_median( t, bpath );
                    }
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        /* Processes overflow of the root

        Splits root into 2 node and leave only their mediane

        @param [out] - transaction
        @retval RetCode - operation status
        */
        [[nodiscard]]
        auto process_overflow_root( Transaction & t ) noexcept
        {
            using namespace std;

            try
            {
                assert( elements_.size() > BTreeMax );

                // split this item into 2 new and save them
                auto l = make_shared< BTree >(), r = make_shared< BTree >();
                split_node( l, r );
                if ( auto[ rc1, rc2 ] = tuple{ l->save( t ), r->save( t ) }; RetCode::Ok != rc1 && RetCode::Ok != rc2 )
                {
                    return max( rc1, rc2 );
                }

                // leave only mediane element...
                elements_[ 0 ] = move( elements_[ BTreeMin ] );
                elements_.resize( 1 );

                // with links to new items
                links_[ 0 ] = l->uid(); links_[ 1 ] = r->uid();
                links_.resize( 2 );

                // ovewrite root node
                return overwrite( t );
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        /* Process overflow of non root node

        Split node into 2 ones and extrude median element to the parent node

        @param [out] t - transaction
        @param [in] bpath - path in btree
        @retval RetCode - operation status
        @throw nothing
        */
        [[nodiscard]]
        auto split_and_araise_median( Transaction & t, BTreePath & bpath )
        {
            using namespace std;

            try
            {
                assert( elements_.size() > BTreeMax );

                // split node into 2 new and save them
                auto l = make_shared< BTree >(), r = make_shared< BTree >();
                split_node( l, r );
                if ( auto[ rc1, rc2 ] = tuple{ l->save( t ), r->save( t ) }; RetCode::Ok != rc1 && RetCode::Ok != rc2 )
                {
                    return max( rc1, rc2 );
                }

                // get parent b-tree path
                BTreePath::value_type parent_path = move( bpath.back() ); bpath.pop_back();

                // remove this node from storage and drop it from cache
                if ( auto[ rc1, rc2 ] = tuple{ cache_->drop( uid_ ), t.erase_chain( uid_ ) }; RetCode::Ok != rc1 && RetCode::Ok != rc2 )
                {
                    return max( rc1, rc2 );
                }

                // and insert mediane element to parent
                if ( auto[ rc, parent ] = cache_->get_node( parent_path.first ); RetCode::Ok != rc )
                {
                    return rc;
                }
                else
                {
                    return parent->insert_araising_element( t, bpath, parent_path.second, l->uid_, move( elements_[ BTreeMin + 1 ] ), r->uid_ );
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        /* Inserts araising element

        Just inserts element and check overflow condition

        @param [out] t - transaction
        @param [in] bpath - path in b-tree
        @param [in] pos - insert position 
        @param [in] l_link - left link of araising element
        @param [in] e - element to be inserted
        @param [in] r_link - right link of the element
        @retval RetCode - operation status
        @throw nothing
        */
        [[nodiscard]]
        auto insert_araising_element(
            Transaction & t,
            BTreePath & bpath,
            Pos pos,
            NodeUid l_link,
            Element && e,
            NodeUid r_link ) noexcept
        {
            using namespace std;

            try
            {
                assert( elements_.size() <= BTreeMax );

                // insert araising element
                elements_.emplace( begin( elements_ ) + pos, move( e ) );
                links_.insert( begin( links_ ) + pos, l_link );
                links_[ pos + 1 ] = r_link;

                // check node for overflow
                if ( elements_.size() > BTreeMax )
                {
                    // if this is root node of b-tree
                    if ( bpath.empty() )
                    {
                        return process_overflow_root( t );
                    }
                    else
                    {
                        return split_and_araise_median( t, bpath );
                    }
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


    public:

        /** Default constructor, creates dummy b-tree node
        */
        BTree() noexcept : uid_( InvalidNodeUid )
        {
            links_.push_back( InvalidNodeUid );
        }


        /** The class is not copyable/movable
        */
        BTree( BTree&& ) = delete;


        /* Explicit constructor
        */
        explicit BTree( NodeUid uid, StorageFile * file, BTreeCache * cache ) noexcept
            : uid_( uid )
            , file_( file )
            , cache_( cache )
        {
            assert( file_ && cache_ );

            try
            {
                // if existing node - load it
                if ( InvalidNodeUid != uid_ )
                {
                    auto isbuf = file_->get_chain_reader( uid_ );
                    std::istream is( &isbuf );
                    boost::archive::binary_iarchive ar( is );
                    ar & *this;

                    uid_ = RetCode::Ok == isbuf.status() ? uid_ : InvalidNodeUid;
                }
            }
            catch ( ... )
            {
                uid_ = InvalidNodeUid;
            }
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
        @param Value - value of the element
        @throw nothing
        */
        const auto & value( size_t ndx ) const noexcept
        {
            assert( ndx < elements_.size() );
            return elements_[ ndx ].value_;
        }

        const auto & good_before( size_t ndx ) const noexcept
        {
            assert( ndx < elements_.size() );
            return elements_[ ndx ].good_before_;
        }

        const auto & children( size_t ndx ) const noexcept
        {
            assert( ndx < elements_.size() );
            return elements_[ ndx ].children_;
        }


        /** Searches through b-tree for given key digest...

        accumulates search path that can be later used as a hint for erase operation.

        @param [in] digest - key to be found
        @param [out] path - search path
        @retval RetCode - operation status
        @retval bool - if key found
        @retval Pos - position of found element or where it could be if exists
        @throw nothing
        */
        std::tuple< RetCode, bool, Pos > find_digest( Digest digest, BTreePath & path ) const noexcept
        {
            using namespace std;

            try
            {
                Element e{ digest };

                auto link = InvalidNodeUid;

                assert( elements_.size() + 1 == links_.size() );

                auto lower = lower_bound( begin( elements_ ), end( elements_ ), e ); lower != end( elements_ );
                size_t d = static_cast< size_t >( std::distance( begin( elements_ ), lower ) );

                if ( lower != end( elements_ ) )
                {
                    if ( lower->digest_ == e.digest_ )
                    {
                        return { RetCode::Ok, true, d };
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
                    return { RetCode::Ok, false, d };
                }
                else
                {
                    assert( cache_ );

                    if ( auto[ rc, p ] = cache_->get_node( link ); RetCode::Ok == rc )
                    {
                        assert( path.size() < path.capacity() );
                        path.emplace_back( uid_, d );

                        assert( p );
                        return p->find_digest( digest, path );
                    }
                    else
                    {
                        return { rc, false, Npos };
                    }
                }
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, false, Npos };
        }


        /** Inserts new subkey with given parameters to the key at specified position

        @param [in] pos - position of key
        @param [in] subkey - subkey digest
        @param [in] value - value to be assigned to new subkey
        @param [in] good_before - expiration mark for the subkey
        @param [in] overwrite - overwrite existing subkey
        @return RetCode - operation status
        @throw nothing
        */
        RetCode insert( Pos pos, Digest digest, Value && value, uint64_t good_before, bool overwrite ) noexcept
        {
            using namespace std;

            assert( pos < elements_.size() );

            try
            {
                Element e{ digest, move( value ), good_before, InvalidNodeUid };

                // open transaction
                if ( auto transaction = file_->open_transaction(); RetCode::Ok == transaction.status() )
                {
                    // get uid of chldren containing b-tree
                    auto children_uid = elements_[ pos ].children_;

                    // if this is not the 1st child?
                    if ( InvalidNodeUid != children_uid )
                    {
                        assert( cache_ );

                        // load children b-tree
                        if ( auto[ rc, children_btree ] = cache_->get_node( children_uid ); RetCode::Ok == rc )
                        {
                            // and insert new element
                            if ( auto rc = children_btree->insert_element( transaction, move( e ), overwrite ); RetCode::Ok != rc )
                            {
                                return rc;
                            }
                        }
                        else
                        {
                            return rc;
                        }
                    }
                    else
                    {
                        // create new b-tree
                        auto children_btree = make_shared< BTree >();

                        // insert new element (it causes saving)
                        if ( auto rc = children_btree->insert_element( transaction, move( e ) ); RetCode::Ok != rc )
                        {
                            return rc;
                        }

                        // get children b-tree uid
                        elements_[ pos ].children_ = children_btree->uid_;

                        // and save this b-tree node with preservation of uid
                        //save_with_preservation();
                    }

                    // commit transaction
                    return transaction.commit();

                }
                else
                {
                    return transaction.status();
                }
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }
    };
}


#endif