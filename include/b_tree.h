#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <memory>
#include <tuple>
#include <algorithm>
#include <limits>
#include <exception>

#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/serialization/split_member.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/collection_size_type.hpp>
#include <boost/archive/binary_iarchive.hpp>
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

        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static_assert( BTreeMinPower >= 2, "B-tree power must be > 1" );
        static constexpr auto BTreeMin = BTreeMinPower - 1;
        static constexpr auto BTreeMax = 2 * BTreeMinPower - 1;

    public:

        using BTreeP = std::shared_ptr< BTree >;
        using NodeUid = typename StorageFile::ChunkUid;

        static constexpr auto RootNodeUid = StorageFile::RootChunkUid;
        static constexpr auto InvalidNodeUid = StorageFile::InvalidChunkUid;

        using Pos = size_t;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };


    private:

        struct Element
        {
            friend class boost::serialization::access;

            Digest digest_;
            Value value_;
            uint64_t good_before_;
            NodeUid children_;

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

            struct var_cmp
            {
                bool value = true;

                template < typename U, typename V >
                void operator () ( const U &, const V & )
                {
                    value = false;
                }

                template < typename T >
                void operator () ( const T & l, const T & r )
                {
                    value = ( l == r );
                }
            };

            friend bool operator == ( const Element & l, const Element & r )
            {
                if ( l.digest_ != r.digest_ || l.good_before_ != r.good_before_ || l.children_ != r.children_ ) return false;
                var_cmp cmp;
                std::visit( cmp, l.value_, r.value_ );
                return cmp.value;
            }

            friend bool operator < ( const Element & l, const Element & r ) noexcept
            {
                return l.digest_ < r.digest_;
            }
        };

        using ElementCollection = boost::container::static_vector< Element, BTreeMax + 1 >;
        using LinkCollection = boost::container::static_vector< NodeUid, BTreeMax + 2 >;

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


        auto save( typename StorageFile::Transaction & t ) const
        {
            if ( !file_ || !cache_ ) throw std::logic_error( "Attempt to save dummy b-tree" );

            auto osbuf = t.get_chain_writer();
            std::ostream os( &osbuf );
            boost::archive::binary_oarchive ar( os );
            ar & *this;
            os.flush();

            if ( RetCode::Ok != t.status() ) throw std::runtime_error( "Unable to save b-tree" );

            NodeUid uid = t.get_first_written_chunk();
            std::swap( uid, const_cast< BTree* >( this )->uid_ );
            cache_->update_uid( uid, uid_ );
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

        auto uid() const noexcept { return uid_; }

        auto & guard() const noexcept { return guard_; }

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

        template < typename BTreePath >
        std::tuple< RetCode, bool, Pos > find_digest( Digest digest, BTreePath & path ) const noexcept
        {
            using namespace std;

            path.push_back( uid_ );

            Element e{ digest };

            auto link = InvalidNodeUid;

            assert( elements_.size() + 1 == links_.size() );

            if ( auto lower = lower_bound( begin( elements_ ), end( elements_ ), e ); lower != end( elements_ ) )
            {
                size_t d = static_cast< size_t >( std::distance( begin( elements_ ), lower ) );

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
                return { RetCode::Ok, false, Npos };
            }
            else
            {
                assert( cache_ );

                if ( auto[ rc, p ] = cache_->get_node( link ); RetCode::Ok == rc )
                {
                    assert( p );
                    return p->find_digest( digest, path );
                }
                else
                {
                    return { rc, false, Npos };
                }
            }
        }
    };
}


#endif