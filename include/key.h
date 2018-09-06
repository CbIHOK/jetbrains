#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <string>
#include <string_view>
#include <regex>
#include <exception>


namespace jb
{
    /** Represent key with fast syntax operation without any copying and heap allocations

    Since the system implies a lot of operations on keys (parsing, splitting, comparing, etc. )
    it makes a sense to introduce a class that makes these operations faster. The idea is to
    fix incoimg key strings and then access to them via std::string_view, that does that is much
    faster cuz it does not allocate heap, copy data, and so on.
    
    @tparam Policies - global settings
    */
    template < typename Policies >
    class Storage< Policies >::Key
    {

    public:

        //
        // public aliases
        //
        using CharT = typename Policies::KeyCharT;
        using ValueT = std::basic_string< CharT >;
        using ViewT = std::basic_string_view< CharT >;

    private:


        //
        // Storage::Hash<> needs access to private memebers
        //
        friend class Storage;
        template < typename T > friend struct Storage::Hash;


        //
        // private aliases
        //
        static constexpr decltype( ViewT::npos ) npos = ViewT::npos;
        static constexpr CharT separator{ '/' };


        //
        // data members
        //
        ViewT view_;
        

        //
        // explicit private constructor
        //
        explicit Key( const ViewT & view ) noexcept : view_( view ) {}


    public:

        /** Default construcor, creates empty key

        @throw nothing
        */
        Key( ) noexcept = default;


        /** Copy constructor

        @throw nothing
        */
        Key( const Key & o ) : view_{ o.view_ }
        {
        }
        

        /** Copy assignment
        */
        Key & operator = ( const Key & o )
        {
            view_ = o.view_;
            return *this;
        }


        /** Move constructor/assignment
        */
        Key( Key && ) noexcept = default;
        Key & operator = ( Key && ) noexcept = default;


        /** Explicit constructor from string

        Validates given string and makes key on its base, source string must stay alive till
        key is in use.

        @throw may throw std::exception
        */
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


        //
        // type conversion
        //
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
        auto empty() const noexcept { return view_.size() == 0; }
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

        @retval Key - the 1st segment of key
        @retval Key - the rest of path
        @throw std::logic_error if the key does not represent valid path
        */
        std::tuple< Key, Key > split_at_head( ) const
        {
            using namespace std;

            if ( is_path() )
            {
                assert( view_.size() );

                if ( auto not_sep = view_.find_first_not_of( separator ); not_sep == ViewT::npos )
                {
                    return { Key{}, Key{} };
                }
                else if ( auto sep = view_.find_first_of( separator, not_sep ); sep == ViewT::npos )
                {
                    return { Key{ view_ }, Key{} };
                }
                else
                {
                    return { Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) } };
                }
            }
            else
            {
                throw logic_error( "The key is not a path" );
            }
        }


        /** Splits given key into two ones by the last segment

        @retval Key - path to the last segment
        @retval Key - the last segment
        @throw std::logic_error if the key does not represent valid path
        */
        std::tuple< Key, Key > split_at_tile( ) const
        {
            using namespace std;

            if ( is_path() )
            {
                if ( auto not_sep = view_.find_last_not_of( separator ); not_sep == ViewT::npos )
                {
                    return { Key{}, Key{} };
                }
                else
                {
                    auto sep = view_.find_last_of( separator );
                    assert( sep != ViewT::npos );

                    return { Key{ view_.substr( 0, sep ) }, Key{ view_.substr( sep ) } };
                }
            }
            else
            {
                throw logic_error( "The key is not a path" );
            }
        }


        /** Checks if this key is subkey for given super key

        and returns relative path from the superkey to the subkey

        @param [in] superkey - superkey to be checked
        @retval bool - if this instance is the subkey
        @retval Key - relative path from the superkey to the subkey
        @throw std::logic_error if the keys do not represent valid path
        */
        std::tuple< bool, Key > is_subkey( const Key & superkey ) const
        {
            using namespace std;

            if ( is_path() && superkey.is_path() )
            {
                if ( superkey == root() && *this != root() )
                {
                    return { true, Key{ view_ } };
                }
                else if ( superkey.view_.size() < view_.size() && 
                          superkey.view_ == view_.substr( 0, superkey.view_.size() ) &&
                          view_[ superkey.view_.size() ] == separator )
                {
                    return { true, Key{ view_.substr( superkey.view_.size() ) } };
                }
                else
                {
                    return { false, Key{} };
                }
            }
            else
            {
                throw std::logic_error( "The key is not a path" );
            }
        }


        /** Checks if this key is superkey for given subkey

        and returns relative path from the superkey to the subkey

        @param [in] subkey - subkey to be checked
        @retval bool - if this instance is the superkey
        @retval Key - relative path from the superkey to the subkey
        @throw std::logic_error if the keys do not represent valid path
        */
        auto is_superkey( const Key & subkey ) const
        {
            return subkey.is_subkey( *this );
        }


        /** Cuts leading separator of path

        @retval Key - the version with removed lead separator
        @throw std::logic_error if the key does not represent valid path
        */
        Key cut_lead_separator() const
        {
            using namespace std;

            if ( is_path( ) )
            {
                return Key{ view_.substr( 1 ) };
            }
            else
            {
                throw logic_error( "The key is not a path" );
            }
        }

    };
}

#endif