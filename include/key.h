#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <filesystem>
#include <memory>
#include <string>
#include <string_view>


namespace jb
{
    template < typename Policies, typename Pad >
    class Key
    {
        template < typename Policies, typename Pad, typename T > friend struct Hash;

        using CharT = typename Policies::KeyCharT;
        using ValueT = std::basic_string< CharT >;
        using ValueP = std::shared_ptr< ValueT >;
        using ViewT = std::basic_string_view< CharT >;

        static constexpr CharT separator{ std::filesystem::path::preferred_separator };

        enum  class Type{ Invalid, Path, Leaf };

        ValueP value_;
        ViewT view_;
        Type type_ = Type::Invalid;
        
        Key( ValueP value, const ViewT & view, Type type ) noexcept : value_( value ), view_( view ), type_( type ) {}


    public:

        Key( ) noexcept = default;

        Key( const Key & ) noexcept = default;
        Key & operator = ( const Key & ) noexcept = default;

        Key( Key && ) noexcept = default;
        Key & operator = ( Key && ) noexcept = default;

        template< typename T > 
        Key( const T & value ) noexcept
        {
            using namespace std;
            using namespace std::filesystem;

            try
            {
                path p{ value };

                if ( ! p.has_root_name() )
                {
                    if ( p.has_root_directory( ) && p.has_relative_path() )
                    {
                        if ( p.has_filename( ) )
                        {
                            value_ = make_shared< ValueT >( move( p.lexically_normal( ).string< CharT >( ) ) );
                        }
                        else
                        {
                            value_ = make_shared< ValueT >( move( p.lexically_normal( ).parent_path( ).string< CharT >( ) ) );
                        }

                        type_ = Type::Path;
                    }
                    else if ( p.has_filename( ) )
                    {
                        value_ = make_shared< ValueT >( move( p.lexically_normal( ).string< CharT >( ) ) );

                        type_ = Type::Leaf;
                    }

                    if ( value_ ) 
                    {
                        view_ = ViewT( value_->data( ), value_->size( ) );
                    }
                }
            }
            catch ( ... )
            {
            }
        }

        bool is_valid( ) const noexcept { return type_ != Type::Invalid;  }
        bool is_path( ) const noexcept { return type_ == Type::Path; }
        bool is_leaf( ) const noexcept { return type_ == Type::Leaf; }

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

            if ( type_ == Type::Path )
            {
                auto sep = view_.find_first_of( separator, 1 );

                if ( sep != ViewT::npos )
                {
                    return tuple{ 
                        true, 
                        Key{ value_, view_.substr( 0, sep ), Type::Path  }, 
                        Key{ value_, view_.substr( sep ), Type::Path }
                    };
                }
                else
                {
                    return tuple{ true, Key{ value_, view_, Type::Path }, Key{} };
                };
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

            if ( type_ == Type::Path )
            {
                auto sep = view_.find_last_of( separator );
                assert( sep != ViewT::npos );

                return tuple{
                    true,
                    Key{ value_, view_.substr( 0, sep ),  Type::Path },
                    Key{ value_, view_.substr( sep + 1 ), Type::Leaf }
                };
            }
            else
            {
                return tuple{ false, Key{}, Key{} };
            }
        }

        auto is_subkey( const Key & superkey ) const noexcept
        {
            if ( Type::Path == type_ && Type::Path == superkey.type_ && superkey.view_.size( ) < view_.size( ) )
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
    struct Hash< Policies, Pad, Key< Policies, Pad > >
    {
        static constexpr bool enabled = true;

        size_t operator() ( const Key< Policies, Pad > & value ) const noexcept
        {
            static constexpr std::hash< typename Key< Policies, Pad >::ViewT > hasher{};
            return hasher( value.view_ );
        }
    };
}

#endif