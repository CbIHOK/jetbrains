#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <memory>
#include <tuple>
#include <algorithm>
#include <limits>

#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/serialization/split_member.hpp>
#include <boost/serialization/nvp.hpp>
#include <boost/serialization/collection_size_type.hpp>
#include <boost/serialization/variant.hpp>

class TestBTree;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::BTree
    {
        friend class TestBTree;
        friend class boost::serialization::access;

        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using Digest = typename Bloom::Digest;

        template < typename T, size_t C > using  static_vector = boost::container::static_vector< T, C >;

    public:

        using BTreeP = std::shared_ptr< BTree >;
        using NodeUid = typename StorageFile::ChunkUid;

        static constexpr auto RootNodeUid = StorageFile::RootChunkUid;
        static constexpr auto InvalidNodeUid = StorageFile::InvalidChunkUid;

        using Pos = size_t;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };

        using BTreePath = boost::container::static_vector < NodeUid, 64 >;

    private:

        struct Element
        {
            Digest digest_;
            Value value_;
            Timestamp expiration_;
            NodeUid children_;

            template<class Archive>
            void serialize( Archive & ar, const unsigned int version )
            {
                ar & digest_;
                ar & value_;
                ar & expiration_;
                ar & children_;
            }

            operator Digest () const noexcept { return digest_; }

            friend bool operator < ( const Element & l, const Element & r ) noexcept
            {
                return l.key_hash_ < r.key_hash_;
            }
        };

        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static_assert( BTreeMinPower >= 2, "B-tree power must be > 1" );

        static constexpr auto BTreeMin = BTreeMinPower - 1;
        static constexpr auto BTreeMax = 2 * BTreeMinPower - 1;

        NodeUid uid_;
        StorageFile * storage_;
        BTreeCache * cache_;
        mutable boost::upgrade_mutex guard_;
        static_vector< Element, BTreeMax + 1> elements_;
        static_vector< NodeUid, BTreeMax + 2> links_;

        template<class Archive>
        void save( Archive & ar, const unsigned int version ) const
        {
            using namespace boost::serialization;

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

        template<class Archive>
        void load( Archive & ar, const unsigned int version )
        {
            collection_size_type count( t.size() );
            ar >> BOOST_SERIALIZATION_NVP( count );
            element_.resize( count );
            links_.resize( count + 1 );
            if ( !elements_.empty() )
            {
                ar >> serialization::make_array< Element, collection_size_type >(
                    static_cast< Element* >( &elements_[ 0 ] ),
                    count
                    );
                ar >> serialization::make_array< NodeUid, collection_size_type >(
                    static_cast< NodeUid* >( &links_[ 0 ] ),
                    count
                    );
            }

        }

        BOOST_SERIALIZATION_SPLIT_MEMBER()

    public:

        /** The class is not copyable/movable
        */
        BTree( BTree&& ) = delete;

        BTree() noexcept
        {
            links_.push_back( InvalidNodeUid );
        }

        BTree( NodeUid uid, StorageFile * file, BTreeCache * cache ) noexcept
            : uid_( uid ),
            , file_( file )
            , cache_( cache )
        {
            assert( file_ && cache_ );
            links_.push_back( InvalidNodeUid );
        }

        auto uid() const noexcept { return uid_; }

        auto & guard() const noexcept { return guard_; }

        auto save( typename StorageFile::Transaction & t ) noexcept
        {
            auto sbuf = t.get_chain_writer();
            std::ostream os( &sbuf );
            boost::archive::binary_oarchive ar( os );
            ar & *this;

            auto new_uid = t.get_first_written_chunk();
            //if ( cache_ ) cache_->update_uid( uid_, new_uid );
            uid_ = new_uid;
        }

        auto load() noexcept
        {
            auto sbuf = file_->get_chain_reader();
            std::ostream is( &sbuf );
            boost::archive::binary_oarchive ar( is );
            ar & *this;
        }
    };
}


#endif