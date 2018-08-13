#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <storage.h>


namespace jb
{
    template < typename CharT >
    class Key
    {
        template < typename T, typename Policies, typename Pad > friend struct Hash;

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

        Key( const std::filesystem::path & key ) noexcept
        {
            using namespace std;

            try
            {
                if ( ! key.has_root_name() )
                {
                    if ( key.has_root_directory( ) && key.has_relative_path() )
                    {
                        if ( key.has_filename( ) )
                        {
                            value_ = make_shared< ValueT >( move( key.lexically_normal( ).string< CharT >( ) ) );
                        }
                        else
                        {
                            value_ = make_shared< ValueT >( move( key.lexically_normal( ).parent_path( ).string< CharT >( ) ) );
                        }

                        type_ = Type::Path;
                    }
                    else if ( key.has_filename( ) )
                    {
                        value_ = make_shared< ValueT >( move( key.lexically_normal( ).string< CharT >( ) ) );

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
                        Key{ value_, view_.substr( 0, sep ),  Type::Path  }, 
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
                    Key{ value_, view_.substr( sep ), Type::Path }
                };
            }
            else
            {
                return tuple{ false, Key{}, Key{} };
            }
        }
    };


    template < typename Policies, typename Pad >
    struct Hash< Policies, Pad, Key< typename Policies::KeyCharT > >
    {
        static constexpr bool enabled = true;

        size_t operator() ( const Key< typename Policies::KeyCharT > & value ) const noexcept
        {
            static constexpr Policies::KeyPolicy::KeyHashF hasher{};
            return hasher( value.view_ );
        }
    };
}

#endif