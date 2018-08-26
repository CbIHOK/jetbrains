#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <string>
#include <string_view>
#include <regex>


namespace jb
{
    template < typename Policies >
    class Storage< Policies >::Key
    {

    public:

        using CharT = typename Policies::KeyCharT;
        using ValueT = std::basic_string< CharT >;
        using ViewT = std::basic_string_view< CharT >;

    private:

        friend class Storage;
        template < typename T > friend struct Storage::Hash;

        static constexpr decltype( ViewT::npos ) npos = ViewT::npos;
        static constexpr CharT separator{ '/' };

        ViewT view_;
        
        explicit Key( const ViewT & view ) noexcept : view_( view ) {}


    public:

        Key( ) noexcept = default;

        Key( const Key & o ) : view_{ o.view_ }
        {
        }
        
        Key & operator = ( const Key & o )
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
        operator ViewT() const { return view_; }

        //
        // Few little helpers
        //
        operator bool() const noexcept { return view_.size() > 0; }
        auto is_valid() const noexcept { return view_.size() > 0; }
        auto is_path() const noexcept { return view_.size() > 0 && view_.front() == separator; }
        auto is_leaf() const noexcept { return view_.size() > 0 && view_.front() != separator; }
        auto data() const noexcept { return view_.data(); }
        auto size() const noexcept { return view_.size(); }
        auto length() const noexcept { return view_.size(); }
        auto empty() const noexcept { return view.size() == 0; }
        auto begin() const noexcept { return view_.begin(); }
        auto end() const noexcept { return view_.end(); }


        /* Provides root key

        @retval Key - root key
        @throw nothing
        */
        auto static root() noexcept
        { 
            static KeyValue r{ separator };
            static Key ret{ r }; 
            return ret;
        }


        /** Compare operators

        @param [in] l - the left part
        @param [in] r - the right part
        @return true if the parts meet the condition
        @throw nothing
        */
        friend auto operator == ( const Key & l, const Key & r ) noexcept { return l.view_ == r.view_; }
        friend auto operator != ( const Key & l, const Key & r ) noexcept { return l.view_ != r.view_; }
        friend auto operator < ( const Key & l, const Key & r ) noexcept { return l.view_ < r.view_; }
        friend auto operator <= ( const Key & l, const Key & r ) noexcept { return l.view_ <= r.view_; }
        friend auto operator >= ( const Key & l, const Key & r ) noexcept { return l.view_ >= r.view_; }
        friend auto operator > ( const Key & l, const Key & r ) noexcept { return l.view_ > r.view_; }


        /* TODO consider does it really need
        */
        friend auto operator / ( const Key & l, const Key & r )
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


        /** Splits given key into two ones by the first segment

        @retval bool - if succeeded
        @retval Key - the 1st segment
        @retval Key - path from the 1st segment
        @throw nothing
        */
        std::tuple< bool, Key, Key > split_at_head( ) const noexcept
        {
            using namespace std;
            using namespace std::filesystem;

            if ( is_path() )
            {
                assert( view_.size() );

                if ( auto not_sep = view_.find_first_not_of( separator ); not_sep == ViewT::npos )
                {
                    return { true, Key{}, Key{} };
                }
                else if ( auto sep = view_.find_first_of( separator, not_sep ); sep == ViewT::npos )
                {
                    return { true, Key{ view_ }, Key{} };
                }
                else
                {
                    return { true, Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) } };
                }
            }
            else
            {
                return { false, Key{}, Key{} };
            }
        }


        /** Splits given key into two ones by the last segment

        @retval bool - if succeeded
        @retval Key - path to the last segment
        @retval Key - the last segment
        @throw nothing
        */
        std::tuple< bool, Key, Key >  split_at_tile( ) const noexcept
        {
            using namespace std;
            using namespace std::filesystem;

            if ( is_path() )
            {
                if ( auto not_sep = view_.find_last_not_of( separator ); not_sep == ViewT::npos )
                {
                    return { true, Key{}, Key{} };
                }
                else
                {
                    auto sep = view_.find_last_of( separator );
                    assert( sep != ViewT::npos );

                    return { true, Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) } };
                }
            }
            else
            {
                return { false, Key{}, Key{} };
            }
        }


        /** Checks if this key is subkey for given super key

        and returns relative path from the superkey to the subkey

        @param [in] superkey - superkey to be checked
        @retval bool - if this instance is the subkey
        @retval Key - relative path from the superkey to the subkey
        @throw nothing
        */
        std::tuple< bool, Key > is_subkey( const Key & superkey ) const noexcept
        {
            using namespace std;

            if ( is_path() )
            {
                if ( superkey == Key{} || superkey == root() )
                {
                    return { true, Key{ view_ } };
                }
                else if ( superkey.view_.size() < view_.size() && superkey.view_ == view_.substr( 0, superkey.view_.size() ) )
                {
                    return { true, Key{ view_.substr( superkey.view_.size() ) } };
                }
                else if ( superkey.view_.size() == view_.size() && superkey.view_ == view_.substr( 0, superkey.view_.size() ) )
                {
                    return { true, move( root() ) };
                }
            }

            return { false, Key{} };
        }


        /** Checks if this key is superkey for given subkey

        and returns relative path from the superkey to the subkey

        @param [in] subkey - subkey to be checked
        @retval bool - if this instance is the superkey
        @retval Key - relative path from the superkey to the subkey
        @throw nothing
        */
        auto is_superkey( const Key & subkey ) const noexcept
        {
            return subkey.is_subkey( *this );
        }


        /** Cuts leading separator of path

        @retval bool - if succeeded
        @retval Key - the version with removed lead separator
        @throw nothing
        */
        std::tuple< bool, Key > cut_lead_separator() const noexcept
        {
            using namespace std;

            if ( is_path( ) )
            {
                return { true, Key{ view_.substr( 1 ) } };
            }

            return { false, Key{} };
        }

    };
}

#endif