#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <filesystem>
#include <string>
#include <string_view>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::Key
    {
        friend typename Pad;
        friend class Storage;
        template < typename Policies, typename Pad, typename T > friend struct Hash;


        using CharT = typename Policies::KeyCharT;
        using ValueT = std::basic_string< CharT >;
        using ViewT = std::basic_string_view< CharT >;

        static constexpr decltype( ViewT::npos ) npos = ViewT::npos;
        static constexpr CharT separator{ std::filesystem::path::preferred_separator };

        ValueT value_;
        ViewT view_;
        
        explicit Key( const ViewT & view ) noexcept : view_( view ) {}


    public:

        Key( ) noexcept = default;

        Key( const Key & ) noexcept = default;
        Key & operator = ( const Key & ) noexcept = default;

        Key( Key && ) noexcept = default;
        Key & operator = ( Key && ) noexcept = default;

        explicit Key( const ValueT & value )
        {
            using namespace std;
            using namespace std::filesystem;

            path p{ value };

            if ( ! p.has_root_name() )
            {
                if ( p.has_root_directory( ) )
                {
                    if ( p.has_filename( ) )
                    {
                        value_ =  move( p.lexically_normal( ).string< CharT >( ) );
                    }
                    else
                    {
                        value_ = move( p.lexically_normal( ).parent_path( ).string< CharT >( ) );
                    }
                }
                else if ( p.has_filename( ) )
                {
                    value_ = move( p.lexically_normal( ).string< CharT >( ) );
                }

                view_ = ViewT( value_.data( ), value_.size( ) );
            }
        }

        operator ValueT( ) const
        {
            return ValueT( view_.cbegin(), view_.cend() ) ;
        }

        operator ViewT( ) const noexcept
        {
            return view_;
        }

        bool is_valid( ) const noexcept { return view_.size() > 0;  }
        bool is_path( ) const noexcept { return view_.size( ) > 0 && view_.front() == separator; }
        bool is_leaf( ) const noexcept { return view_.size( ) > 0 && view_.front() != separator; }

        friend bool operator == ( const Key & l, const Key & r ) noexcept { return l.view_ == r.view_; }
        friend bool operator != ( const Key & l, const Key & r ) noexcept { return l.view_ != r.view_; }
        friend bool operator < ( const Key & l, const Key & r ) noexcept { return l.view_ < r.view_; }
        friend bool operator <= ( const Key & l, const Key & r ) noexcept { return l.view_ <= r.view_; }
        friend bool operator >= ( const Key & l, const Key & r ) noexcept { return l.view_ >= r.view_; }
        friend bool operator > ( const Key & l, const Key & r ) noexcept { return l.view_ > r.view_; }

        auto split_at_head( ) const
        {
            using namespace std;
            using namespace std::filesystem;

            if ( is_path() )
            {
                assert( view_.size() );

                if ( auto not_sep = view_.find_first_not_of( separator ); not_sep == ViewT::npos )
                {
                    return tuple{ true, Key{}, Key{} };
                }
                else if ( auto sep = view_.find_first_of( separator, not_sep ); sep == ViewT::npos )
                {
                    return tuple{ true, Key{ view_ }, Key{} };
                }
                else
                {
                    return tuple{ true, Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) } };
                }
            }
            else
            {
                return tuple{ false, Key{}, Key{} };
            }
        }

        auto split_at_tile( ) const
        {
            using namespace std;
            using namespace std::filesystem;

            if ( is_path() )
            {
                if ( auto not_sep = view_.find_last_not_of( separator ); not_sep == ViewT::npos )
                {
                    return tuple{ true, Key{}, Key{} };
                }
                else
                {
                    auto sep = view_.find_last_of( separator );
                    assert( sep != ViewT::npos );

                    return tuple{
                        true,
                        Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) }
                    };
                }
            }
            else
            {
                return tuple{ false, Key{}, Key{} };
            }
        }

        auto is_subkey( const Key & superkey ) const noexcept
        {
            if ( is_path() && superkey.is_path() && superkey.view_.size( ) < view_.size( ) )
            {
                return superkey.view_ == view_.substr( 0, superkey.view_.size( ) );
            }
            return false;
        }

        auto is_superkey( const Key & subkey ) const noexcept
        {
            return subkey.is_subkey( *this );
        }
    };


    template < typename Policies, typename Pad >
    struct Hash< Policies, Pad, typename Storage< Policies, Pad >::Key >
    {
        static constexpr bool enabled = true;
        using Key = typename Storage< Policies, Pad >::Key;

        size_t operator() ( const Key & key ) const noexcept
        {
            static constexpr std::hash< Key::ViewT > hasher{};
            return hasher( key.view_ );
        }
    };
}

#endif