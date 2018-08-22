#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <memory>
#include <tuple>
#include <algorithm>
#include <limits>
#include <boost/container/static_vector.hpp>
#include <boost/thread/shared_mutex.hpp>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PhysicalStorage::BTree
    {
    public:

        using Storage = Storage< Policies, Pad >;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using KeyHashT = size_t;
        using BTreeP = std::shared_ptr< BTree >;

        using NodeUid = PhysicalStorage::NodeUid;
        static constexpr auto RootNodeUid = PhysicalStorage::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalStorage::InvalidNodeUid;

        typedef size_t Pos;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };

        struct Element
        {
            KeyHashT key_hash_;
            Value value_;
            Timestamp expiration_;
            NodeUid children_;

            operator KeyHashT () const noexcept { return key_hash_; }

            friend bool operator < ( const Element & l, const Element & r ) noexcept
            {
                return l.key_hash_ < r.key_hash_;
            }
        };

    private:

        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static_assert( BTreeMinPower >= 2, "B-tree power must be > 1" );

        static constexpr auto BTreeMin = BTreeMinPower - 1;
        static constexpr auto BTreeMax = 2 * BTreeMinPower - 1;

        PhysicalStorage * storage_;
        NodeUid parent_uid_;
        NodeUid uid_;
        mutable boost::upgrade_mutex guard_;
        boost::container::static_vector< Element, BTreeMax + 1> elements_;
        boost::container::static_vector< NodeUid, BTreeMax + 2> links_;

        
    public:

        /** The class is not copyable/movable
        */
        BTree( BTree&& ) = delete;


        BTree( )
        {
            links_.push_back( InvalidNodeUid );
        }

        auto uid() const noexcept { return uid_; }


        auto & guard( ) const noexcept { return guard_; }


        /** Checks if key presents in the node

        If key presents returns

        @retval Pos - position of the key in the B-tree node or Npos if key is not in the node
        @retval NodeUid - B-tree node to search
        @throw may cause std::exception for some reasons
        */
        std::tuple< Pos, NodeUid > find_key( const Key & key ) const
        {
            using namespace std;

            assert( key.is_leaf() );

            static constexpr Hash< Key > hasher;
            auto hash = hasher( key );

            if ( auto lower = lower_bound( begin( elements_ ), end( elements_ ), hash ); lower == elements_.end() )
            {
                return tuple{ Npos, links_.back() };
            }
            else
            {
                assert( 0 <= distance( begin( elements_ ), lower ) );
                auto pos = static_cast< Pos >( distance( begin( elements_ ), lower ) );
                assert( pos < elements_.size() );

                if ( lower->key_hash_ == hash )
                {
                    return tuple{ pos, InvalidNodeUid };
                }
                else
                {
                    return tuple{ Npos, links_[ pos ] };
                }
            }
        }


        const Value & value( size_t ndx ) const noexcept
        { 
            assert( ndx < elements_.size() );
            return elements_[ ndx ].value_;
        }


        const Timestamp & expiration( size_t ndx ) const noexcept
        {
            assert( ndx < elements_.size( ) );
            return elements_[ ndx ].expiration_;
        }


        NodeUid children( size_t ndx ) const noexcept
        {
            assert( ndx < elements_.size() );
            return elements_[ ndx ].children_;
        }


        std::tuple< RetCode > insert( const Key & key, Value && value, Timestamp && expiration, bool overwrite ) noexcept
        {
            using namespace std;


            return { RetCode::NotImplementedYet };
        }


        std::tuple< RetCode >  erase( size_t ndx ) noexcept
        {
            return { RetCode::NotImplementedYet };
        }
    };
}


#endif