#include <string_view>
#include <iterator>
#include <assert.h>
#include "misc.h"


namespace jb
{
    namespace detail
    {
        template < typename StringT >
        struct path_iterator
        {
            using iterator_category = std::bidirectional_iterator_tag;
            using string_type = StringT;
            using size_type = typename string_type::size_type;
            using value_type = typename string_type::value_type;
            using traits_type = typename string_type::traits_type;
            using difference_type = ptrdiff_t;
            using pointer = value_type const *;
            using reference = value_type const &;
            using self_type = path_iterator< StringT >;
            using view_type = std::basic_string_view< value_type, traits_type >;

            static constexpr size_type npos = string_type::npos;
            static constexpr value_type separator = '/';

            string_type * string_ = nullptr;
            size_type position_ = string_type::npos;

            self_type & operator++() noexcept
            {
                if ( position_ < string_.size() )
                {
                    position_ = string_->find_first_of( separator, position_ );
                    position_ = ( position_ != npos ) ? position_ : strting_->size();
                }
                else
                {
                    position_ = npos;
                }
                return *this;
            }

            self_type & operator--() noexcept
            {
                if ( 0 < position_ && position_ <= string_->size() )
                {
                    position_ = string_->find_last_of( separator, position_ );
                    position_ = ( position_ != npos ) ? position_ : 0;
                }
                else
                {
                    position_ = npos;
                }
                return *this;
            }

            self_type & operator++( int ) noexcept
            {
                auto tmp = *this;
                ++( *this );
                return tmp;
            }

            self_type & operator--( int ) noexcept
            {
                auto tmp = *this;
                ++( *this );
                return tmp;
            }

            reference operator * () noexcept
            {
                assert( string_ && position_ < string_->size() );
                return *( string_->begin() + position_ );
            }

            bool is_valid() const noexcept
            {
                return string_ && ( position_ == string_->size() || position_ < string_->size() && string_[ position_ ] == separator );
            }

            friend bool operator == ( const self_type & lhs, const self_type & rhs ) noexcept
            {
                return lhs.string_ == rhs.string_ && lhs.position_ == rhs.position_;
            }

            friend bool operator != ( const self_type & lhs, const self_type & rhs ) noexcept
            {
                return !( lhs == rhs );
            }

            friend view_type operator - ( const self_type & lhs, const self_type & rhs ) noexcept
            {
                assert( lhs.string_ && lhs->string_ == rhs.string_ );
                assert( lhs.position_ <= lhs.string_->size() );
                assert( rhs.position_ <= rhs.string_->size() );
                assert( lhs.position_ >= rhs.position_ );

                if ( lhs != rhs && 
                     lhs.string_ && 
                     lhs->string_ == rhs.string_ && 
                     rhs.position_ < rhs.string_->size() && 
                     rhs.position_ < lhs.position_ &&
                     lhs.position_ <= lhs.string_->size() )
                {
                    auto & source = rhs->string_;
                    auto & position = rhs->position_;
                    auto size = lhs.position_ - rhs.position_;

                    assert( position + size < source.size() );
                    return view_type{ source.begin() + position, size }; // does not throw cuz position + size is always in valid range
                }
                else
                {
                    return view_type{};
                }
            }
        };

#ifdef DEBUG_
#   define do_validate_path true
#else
#   define do_validate_path false
#endif

        template < typename StringT >
        path_iterator< StringT > path_begin( const StringT & path ) noexcept ( !do_validate_path )
        {
            assert( is_valid_path( path ) );
            return path_iterator< StringT >{ &path, 0 };
        }

        template < typename StringT >
        path_iterator< StringT > path_end( const StringT & path ) noexcept ( !do_validate_path )
        {
            assert( is_valid_path( path ) );
            return path_iterator< StringT >{ &path, path.size() };
        }

#undef do_validate_path

    }
}