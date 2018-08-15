#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <string>
#include <string_view>
#include <regex>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::Key
    {

    public:

        using CharT = typename Policies::KeyCharT;
        using ValueT = std::basic_string< CharT >;
        using ViewT = std::basic_string_view< CharT >;

    private:

        friend typename Pad;
        friend class Storage;
        template < typename Policies, typename Pad, typename T > friend struct Hash;

        static constexpr decltype( ViewT::npos ) npos = ViewT::npos;
        static constexpr CharT separator{ '/' };

        ViewT view_;
        
        explicit Key( const ViewT & view ) noexcept : view_( view ) {}


    public:

        Key( ) noexcept = default;

        Key( const Key & o ) : view_{ o.view_ }
        {
        }
        
        Key & operator = ( const Key & )
        {
            view_ = o.view_;
            return *this;
        }

        Key( Key && ) noexcept = default;
        Key & operator = ( Key && ) noexcept = default;

        explicit Key( const ValueT & value )
        {
            using namespace std;
            
            static auto regexp = [=] () {
                using RegexT = basic_regex< CharT >;
                using RegexStrT = typename RegexT::string_type;

                auto pattern = R"noesc(^([a-zA-Z][\w-]*)$|^(\/[a-zA-Z][\w-]*)+$|^\/$)noesc"s;
                return RegexT{ RegexStrT{ pattern.begin(), pattern.end() } };
            }();

            if ( regex_match( value, regexp ) )
            {
                view_ = ViewT( value.data(), value.size() );
            }
        }

        operator ValueT() const { return ValueT{ cbegin( view_ ), cend( view_ ) }; }
        operator ViewT( ) const { return view_; }

        auto is_valid( ) const noexcept { return view_.size() > 0; }
        auto is_path( ) const noexcept { return view_.size( ) > 0 && view_.front() == separator; }
        auto is_leaf( ) const noexcept { return view_.size( ) > 0 && view_.front() != separator; }
        auto data( ) const noexcept { return view_.data( ); }
        auto size( ) const noexcept { return view_.size( ); }
        auto length( ) const noexcept { return view_.size( ); }
        auto begin( ) const noexcept { return view_.begin( ); }
        auto end( ) const noexcept { return view_.end( ); }

        auto static root()
        { 
            static KeyValue r{ separator };
            static Key ret{ r }; 
            return ret;
        }

        friend auto operator == ( const Key & l, const Key & r ) noexcept { return l.view_ == r.view_; }
        friend auto operator != ( const Key & l, const Key & r ) noexcept { return l.view_ != r.view_; }
        friend auto operator < ( const Key & l, const Key & r ) noexcept { return l.view_ < r.view_; }
        friend auto operator <= ( const Key & l, const Key & r ) noexcept { return l.view_ <= r.view_; }
        friend auto operator >= ( const Key & l, const Key & r ) noexcept { return l.view_ >= r.view_; }
        friend auto operator > ( const Key & l, const Key & r ) noexcept { return l.view_ > r.view_; }

        friend auto operator / ( const Key & l, const Key & r ) noexcept
        {
            if ( l == root() || r.is_path() )
            {
                return ( ValueT )l.view_ + ( ValueT )r.view_;
            }
            else if ( r.is_leaf() )
            {
                return ( ValueT )l.view_ + separator + ( ValueT )r.view_;
            }
            else
            {
                assert( false );
                return ValueT{};
            }
        }

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
            using namespace std;

            if ( is_path() )
            {
                if ( superkey == Key{} || superkey == root() )
                {
                    return tuple{ true, Key{ view_ } };
                }
                else if ( superkey.view_.size() < view_.size() && superkey.view_ == view_.substr( 0, superkey.view_.size() ) )
                {
                    return tuple{ true, Key{ view_.substr( superkey.view_.size() ) } };
                }
            }

            return tuple{ false, Key{} };
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