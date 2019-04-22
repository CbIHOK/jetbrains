#include <string_view>


namespace jb
{
    namespace detail
    {
        template < typename CharT, typename Traits = std::char_traits< CharT > >
        class merged_string_view
        {
            using view = std::basic_string_view < CharT, Traits >;

            view prefix_;
            view suffix_;

        public:

            struct iterator;
            struct reverse_iterator;

            using traits_type = Traits;
            using value_type = CharT;
            using pointer = CharT * ;
            using const_pointer = const CharT*;
            using reference = CharT & ;
            using const_reference = const CharT&;
            using const_iterator = iterator;
            using const_reverse_iterator = reverse_iterator;
            using size_type = std::size_t;
            using difference_type = std::ptrdiff_t;

            constexpr static size_type npos = view::npos;

            constexpr merged_string_view() noexcept {}
            constexpr merged_string_view( const merged_string_view & other ) noexcept = default;
            constexpr merged_string_view( view prefix, view suffix ) noexcept : prefix_( prefix ), suffix_( suffix ) {}

            constexpr merged_string_view & operator = ( const merged_string_view & other ) noexcept = default;

            constexpr iterator begin() const noexcept
            {
                return iterator( this, prefix.begin() );
            }

            constexpr const_iterator cbegin() const noexcept
            {
                return begin();
            }

            constexpr iterator end() const noexcept
            {
                return iterator( this, suffix_ ? suffix_.end() : prefix_.end() );
            }

            constexpr const_iterator cend() const noexcept
            {
                return end();
            }

            constexpr reverse_iterator rbegin() const noexcept
            {
                return reverse_iterator( this, suffix_ ? suffix_.rbegin() : prefix_.rbegin() );
            }

            constexpr const_reverse_iterator crbegin() const noexcept
            {
                return rbegin();
            }

            constexpr reverse_iterator rend() const noexcept
            {
                return reverse_iterator( this, prefix_.rend() );
            }

            constexpr const_reverse_iterator crend() const noexcept
            {
                return rend();
            }

            constexpr const_reference operator[] ( size_type pos ) const noexcept
            {
                return pos < prefix_.size() ? prefix_[ pos ] : suffix_[ pos - prefix_.size() ];
            }

            constexpr const_reference at( size_type pos ) const
            {
                return pos < prefix_.size() ? prefix_.at( pos ) : suffix_.at( pos - prefix_.size() );
            }

            constexpr const_reference front() const
            {
                return prefix_.empty() ? suffix_.front() : prefix_.front();
            }

            constexpr const_reference back() const
            {
                return suffix_.empty() ? prefix_.back() : suffix_.back();
            }

            constexpr size_type size() const noexcept
            {
                return prefix_.size() + suffix_.size();
            }

            constexpr size_type length() const noexcept
            {
                return size();
            }

            constexpr size_type max_size() const noexcept
            {
                return prefix_.max_size();
            }

            constexpr bool empty() const noexcept
            {
                return prefix_.empty() && suffix_.empty();
            }

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
                size_t i = 0;

                for ( ; i < count && pos + i < prefix_.size(); ++i, ++dst )
                {
                    *dst = prefix_[ pos + i ];
                }

                for ( ; i < count && pos + i - prefix_.size() < suffix_.size(); ++i, ++dst )
                {
                    *dst = suffix_[ pos + i - prefix_.size() ];
                }

                return i;
            }

            constexpr merged_string_view substr( size_type pos = 0, size_type count = npos ) noexcept
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
    }
}
