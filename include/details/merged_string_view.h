#include <string_view>
#include <iterator>
#include <assert.h>

namespace jb
{
    namespace details
    {
        template < typename View > struct merged_string_view_iterator_base;
        template < typename View > struct merged_string_view_reverse_iterator_base;
        template < typename Base > struct merged_string_view_iterator_decorator;


        template < typename CharT, typename Traits = std::char_traits< CharT > >
        class merged_string_view
        {
            using view = std::basic_string_view < CharT, Traits >;
            view prefix_, suffix_;

        public:

            using self_type = merged_string_view;
            using traits_type = Traits;
            using value_type = CharT;
            using pointer = CharT * ;
            using const_pointer = const CharT*;
            using reference = CharT & ;
            using const_reference = const CharT &;
            using iterator = merged_string_view_iterator_decorator< merged_string_view_iterator_base < self_type > >;
            using const_iterator = iterator;
            using reverse_iterator = merged_string_view_iterator_decorator< merged_string_view_reverse_iterator_base < self_type > >;
            using const_reverse_iterator = reverse_iterator;
            using size_type = std::size_t;
            using difference_type = std::ptrdiff_t;

            constexpr static size_type npos = view::npos;

            constexpr merged_string_view() noexcept {}
            constexpr merged_string_view( const merged_string_view & other ) noexcept = default;
            constexpr merged_string_view( view prefix, view suffix ) noexcept : prefix_( prefix ), suffix_( suffix ) {}

            constexpr merged_string_view & operator = ( const merged_string_view & other ) noexcept = default;

            constexpr iterator begin() const noexcept { return iterator( this, 0 ); }
            constexpr const_iterator cbegin() const noexcept { return begin(); }
            constexpr iterator end() const noexcept { return iterator( this, prefix_.size() + suffix_.size() ); }
            constexpr const_iterator cend() const noexcept { return end(); }
            constexpr reverse_iterator rbegin() const noexcept { return reverse_iterator( this, prefix_.size() + suffix_.size() ); }
            constexpr const_reverse_iterator crbegin() const noexcept { return rbegin(); }
            constexpr reverse_iterator rend() const noexcept { return reverse_iterator( this, 0 ); }
            constexpr const_reverse_iterator crend() const noexcept { return rend(); }

            constexpr const_reference operator[] ( size_type pos ) const noexcept { return pos < prefix_.size() ? prefix_[ pos ] : suffix_[ pos - prefix_.size() ]; }
            constexpr const_reference at( size_type pos ) const { return pos < prefix_.size() ? prefix_.at( pos ) : suffix_.at( pos - prefix_.size() ); }
            constexpr const_reference front() const { return prefix_.empty() ? suffix_.front() : prefix_.front(); }
            constexpr const_reference back() const { return suffix_.empty() ? prefix_.back() : suffix_.back(); }

            constexpr size_type size() const noexcept { return prefix_.size() + suffix_.size(); }
            constexpr size_type length() const noexcept { return size(); }
            constexpr size_type max_size() const noexcept { return prefix_.max_size(); }
            constexpr bool empty() const noexcept { return prefix_.empty() && suffix_.empty(); }

            void remove_prefix( size_type n ) noexcept
            {
                if ( n < prefix_.size() )
                {
                    prefix_.remove_prefix( n );
                }
                else
                {
                    if ( n - prefix_.size() < suffix_.size() )
                    {
                        suffix_.remove_prefix( n - prefix_.size() );
                    }
                    else
                    {
                        suffix_ = view{};
                    }

                    prefix_ = view{};
                }
            }

            void remove_suffix( size_t n ) noexcept
            {
                if ( n < suffix_.size() )
                {
                    suffix_.remove_suffix( n );
                }
                else
                {
                    if ( n - suffix_.size() < prefix_.size() )
                    {
                        prefix_.remove_suffix( n - suffix_.size() );
                    }
                    else
                    {
                        prefix_ = view{};
                    }

                    suffix_ = view{};
                }
            }

            constexpr void swap( merged_string_view & other ) noexcept
            {
                prefix_.swap( prefix_ );
                suffix_.swap( suffix_ );
            }

            size_type copy( CharT * dst, size_type count, size_type pos = 0 ) noexcept
            {
                assert( dst );
                //
                pos = std::min( pos, size() );
                count = std::min( count, size() - pos );
                //
                size_t prefix_pos = 0, prefix_count = 0, suffix_pos = 0, suffix_count = 0;
                //
                if ( pos < prefix_.size() )
                {
                    prefix_pos = pos;
                    prefix_count = std::min( count, prefix.size() - prefix_pos );
                    suffix_count = count - prefix_count;
                }
                else
                {
                    suffix_pos = pos - prefix_.size();
                    suffix_count = count - pos;
                }
                
                prefix_.copy( dst, prefix_count, prefix_pos );
                suffix_.copy( dst + prefix_count, suffix_count, suffix_pos );
                //
                return count;
            }

            constexpr self_type substr( size_type pos = 0, size_type count = npos ) noexcept
            {
                merged_string_view result;

                count = ( count != npos ) ? count : size();

                if constexpr ( pos < prefix_.size() )
                {
                    if constexpr ( pos + count < prefix_.size() )
                    {
                        result = merged_string_view{ prefix_.substr( pos, count ), view{} };
                    }
                    else
                    {
                        auto suffix_count = std::min( suffix_.size(), count + pos - prefix_.size() );
                        result = merged_string_view{ prefix_.substr( pos ), suffix_.substr( 0, suffix_count ) };
                    }
                }
                else if constexpr ( pos - prefix_.size() < suffix_.size() )
                {
                    auto suffix_count = std::min( suffix_.size(), count + pos - prefix_.size() );
                    result = merged_string_view{ view{}, suffix_.substr( pos - prefix_.size(), suffix_count ) };
                }

                return result;
            }
        };


        template < typename View >
        struct merged_string_view_iterator_base
        {
            using view_type = View;
            using size_type = typename view_type::size_type;
            using value_type = typename View::value_type;
            using difference_type = ptrdiff_t;
            using pointer = typename View::const_pointer;
            using reference = typename View::const_reference;
            using iterator_category = std::bidirectional_iterator_tag;

            const view_type * view_ = nullptr;
            size_type pos_ = 0;

            void increase() noexcept { ++pos_; }
            void decrease() noexcept { --pos_; }

            reference operator * () const noexcept 
            {
                assert( view_ && pos_ < view_->size() );
                return ( *view_ )[ pos_ ];
            }
        };


        template < typename View >
        struct merged_string_view_reverse_iterator_base : public merged_string_view_iterator_base< View >
        {

            void increase() noexcept { --pos_; }
            void decrease() noexcept { ++pos_; }

            reference operator * () const noexcept
            {
                assert( view_ && 0 < pos_ && pos_ <= view_->size() );
                return ( *view_ )[ pos_ - 1 ];
            }
        };


        template < typename Base >
        struct merged_string_view_iterator_decorator : Base
        {
            using self_type = merged_string_view_iterator_decorator< Base >;
            using view_type = typename Base::view_type;
            using size_type = typename Base::size_type;
            using value_type = typename Base::value_type;
            using difference_type = typename Base::difference_type;
            using pointer = typename Base::pointer;
            using reference = typename Base::reference;
            using iterator_category = typename Base::iterator_category;

            explicit merged_string_view_iterator_decorator( const view_type * view, size_type pos ) noexcept : Base{ view , pos }
            {
                assert( view_ && pos_ <= view_->size() );
            }

            friend bool operator == ( const self_type & lhs, const self_type & rhs ) noexcept { return lhs.view_ == rhs.view_ && lhs.pos_ == rhs.pos_; }
            friend bool operator != ( const self_type & lhs, const self_type & rhs ) noexcept { return !( lhs == rhs ); }

            self_type & operator++() noexcept { increase(); return *this; }
            self_type operator++( int ) noexcept { self_type tmp = *this; increase(); return tmp; }
            self_type & operator--() noexcept { decrease(); return *this; }
            self_type operator--( int ) noexcept { self_type tmp = *this; decrease(); return tmp; }
            void swap( self_type & other ) noexcept { std::swap( view_, other.view_ ); std::swap( pos_, other.pos_ ); }

            using Base::operator*;
        };
    }
}
