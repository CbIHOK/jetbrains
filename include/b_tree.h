#ifndef __JB__B_TREE__H__
#define __JB__B_TREE__H__


#include <tuple>
#include <algorithm>
#include <mutex>
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
        using KeyHashT = decltype( Hash< Policies, Pad, Key >{}( Key{} ) );

        using NodeUid = PhysicalStorage::NodeUid;
        static constexpr auto RootNodeUid = PhysicalStorage::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalStorage::InvalidNodeUid;

        typedef size_t Pos;
        static constexpr auto Npos = Pos{ std::numeric_limits< size_t >::max() };

    private:

        static constexpr auto BTreePower = Policies::PhysicalVolumePolicy::BTreePower;
        static_assert( BTreePower >= 2, "B-tree power must be >1" );
        static_assert( BTreePower + 1 < std::numeric_limits< ptrdiff_t >::max( ), "B-tree power is too great" );
        
        PhysicalStorage * storage_;
        NodeUid parent_uid_;
        NodeUid uid_;
        mutable boost::upgrade_mutex guard_;
        boost::container::static_vector< KeyHashT, BTreePower> hashes_;
        boost::container::static_vector< Value, BTreePower > values_;
        boost::container::static_vector< Timestamp, BTreePower > expirations_;
        boost::container::static_vector< NodeUid, BTreePower > children_;
        boost::container::static_vector< NodeUid, BTreePower + 1 > links_;
        bool changed = false;


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

        Value value( size_t ndx ) const noexcept
        { 
            assert( ndx < values_.size( ) );
            return move( values_[ ndx ] );
        }

        Timestamp expiration( size_t ndx ) const noexcept
        {
            assert( ndx < expirations_.size( ) );
            return move( expirations_[ ndx ] );
        }

        NodeUid child( size_t ndx ) const noexcept
        {
            assert( ndx < children_.size( ) );
            return move( children_[ ndx ] );
        }

        RetCode erase( size_t ndx ) noexcept
        {
            return RetCode::NotImplementedYet;
        }

        RetCode insert( const Key & key, Value && value, Timestamp && expiration, bool overwrite ) noexcept
        {
            using namespace std;

            assert( key.is_leaf() );
            assert( hashes_.size() == values_.size() );
            assert( hashes_.size() == expirations_.size() );
            assert( hashes_.size() == children_.size() );
            assert( hashes_.size() + 1 == links_.size() );

            try
            {
                if ( expiration != Timestamp{} && expiration < Timestamp::clock::now() )
                {
                    return RetCode::AlreadyExpired;
                }

                static constexpr Hash< Policies, Pad, Key > hasher;
                auto hash = hasher( key );

                if ( hashes_.size() < hashes_.capacity() )
                {
                    auto lower = lower_bound( begin( hashes_ ), end( hashes_ ), hash );

                    if ( *lower != hash )
                    {
                        auto [ hash_ok, hash_it ] = hashes_.insert( lower, hash );
                        assert( hash_ok );

                        auto[ value_ok, value_it ] = values_.emplace( lower, move( value ) );
                        assert( value_ok );

                        auto [ expiration_ok, expiration_it ] = expirations_.emplace{ lower, move( expiration ) };
                        assert( expiration_ok );

                        auto[ child_ok, child_it ] = children_.insert( lower, InvalidNodeUid );
                        assert( child_ok );

                        if ( !( value_ok && value_ok  && expiration_ok &&  child_ok ) )
                        {
                            throw std::runtime_error( "Some shit happens" );
                        }
                    }
                    else if ( overwrite )
                    {
                        assert( 0 <= distance( begin( hashes_ ), lower ) );
                        auto pos = static_cast< Pos >( distance( begin( hashes_ ), lower ) );

                        value_[ pos ] = value;
                        if ( expiration != Timestamp{} ) expirations_[ pos ] = expiration;
                    }
                    else
                    {
                        return RetCode::AlreadyExists;
                    }
                }
                else
                {
                    return RetCode::NotImplementedYet;
                }

            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }

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
            
            assert( hashes_.size() == values_.size() );
            assert( hashes_.size() == expirations_.size() );
            assert( hashes_.size() == children_.size() );
            assert( hashes_.size() + 1 == links_.size() );

            static constexpr Hash< Policies, Pad, Key > hasher;
            auto hash = hasher( key );

            if ( auto lower = lower_bound( begin( hashes_ ), end( hashes_ ), hash ); lower == hashes_.end() )
            {
                return tuple{ Npos, links_.back() };
            }
            else
            {
                assert( 0 <= distance( begin( hashes_ ), lower ) );
                auto pos = static_cast< Pos >( distance( begin( hashes_ ), lower ) );
                assert( pos < hashes_.size() );
                
                if ( *lower == hash )
                {
                    return tuple{ pos, InvalidNodeUid };
                }
                else
                {
                    return tuple{ Npos, links_[ pos ] };
                }
            }
        }
    };
}


#endif