#ifndef __JB__PACKED_VALUE__H__
#define __JB__PACKED_VALUE__H__


#include <type_traits>
#include <variant>
#include <iostream>


#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


class TestPackedValue;


namespace jb
{

    /** Let us know if values of the type are to be saved as BLOB

    @tparam T - type to be checked
    */
    template < typename T >
    struct is_blob_type
    {
        static constexpr bool value = ! std::is_integral_v< T >;
        using StreamCharT = char;
    };


    //
    // Makes the system to consider float as not blob type
    //
    template <>
    struct is_blob_type< float >
    {
        static constexpr bool value = false;
        using StreamCharT = char;
    };


    //
    // Makes the system to consider double as not blob type
    //
    template <>
    struct is_blob_type< double >
    {
        static constexpr bool value = false;
        using StreamCharT = char;
    };


    //
    // Makes the system to consider std::string as a blob type
    //
    template <>
    struct is_blob_type< std::string >
    {
        static constexpr bool value = true;
        using StreamCharT = char;
    };


    //
    // Makes the system to consider std::wstring as a blob type
    //
    template <>
    struct is_blob_type< std::wstring >
    {
        static constexpr bool value = true;
        using StreamCharT = wchar_t;
    };


    //
    // Makes std::basic_string< char16_t > to be considered as BLOB type
    //
    template <>
    struct is_blob_type< std::basic_string< char16_t > >
    {
        static constexpr bool value = true;
        using StreamCharT = char16_t;
    };


    //
    // Makes std::basic_string< char32_t > to be considered as BLOB type
    //
    template <>
    struct is_blob_type< std::basic_string< char32_t > >
    {
        static constexpr bool value = true;
        using StreamCharT = char32_t;
    };


    /** Represent value inside b-tree node

    Since the system uses B-tree for indexing, it does not seem as a good idea to hold
    massive data pieces inside B-tree node, cuz element value stays unnecessary most of
    times. This structure handles the situation of massive data piece and store it separately
    from a B-tree node bringing to node only 2 uint64_t values: value type index and reference
    stored BLOB. If nevetheless the value can be saved inside uint64_t the structure hold it
    in place

    @taparam Policies - global settings
    */
    template < typename Policies >
    struct Storage< Policies >::PhysicalVolumeImpl::BTree::PackedValue
    {
        friend class TestPackedValue;
        friend class BTree;

        using Value = typename Storage::Value;
        using big_uint64_t = boost::endian::big_uint64_at;

        uint64_t type_index_;
        uint64_t value_;


        /* Checks if assigned value is BLOB object

        @retval bool - true if value represented as a BLOB
        @throw nothing
        */
        template < size_t I >
        bool check_for_blob() const
        {
            using namespace std;

            if ( I == type_index_ )
            {
                using value_type = variant_alternative_t< I, Value >;

                return is_blob_type< value_type >::value;
            }
            else
            {
                return check_for_blob< I + 1 >();
            }
        }

        template <>
        bool check_for_blob< std::variant_size_v< Value > >() const
        {
            throw_btree_error( false, RetCode::InvalidData, "Unable to resolve type index" );

            return false;
        }


        /* Pack a value

        @param [in] t - active transaction
        @param [in] value - value to be packed
        @retval PackedValue - packed value
        @throw btree_error, storage_file_serror
        */
        template < size_t I >
        static PackedValue pack( Transaction & t, const Value & value )
        {
            using namespace std;

            if ( I == value.index() )
            {
                using value_type = variant_alternative_t< I, Value >;
                const value_type & typed_value = std::get< I >( value );

                if constexpr ( is_blob_type< value_type >::value )
                {
                    using StreamCharT = typename is_blob_type< value_type >::StreamCharT;

                    auto buffer = t.get_chain_writer< StreamCharT >();
                    std::basic_ostream< StreamCharT > os( &buffer );

                    os << typed_value;
                    os.flush();

                    return PackedValue{ value.index(), t.get_first_written_chunk() };
                }
                else
                {
                    //static_assert( ! is_blob_type< typed_value >::value || ( typed_value ) <= sizeof( uint64_t ) );

                    uint64_t v_{};
                    copy(
                        reinterpret_cast< const char* >( &typed_value ),
                        reinterpret_cast< const char* >( &typed_value ) + sizeof( typed_value ),
                        reinterpret_cast< char* >( &v_ ) );

                    return PackedValue{ value.index(), v_ };
                }
            }
            else
            {
                return pack< I + 1 >( t, value );
            }
        }


        //
        // terminal specialization of pack<>()
        //
        template <>
        static PackedValue pack< std::variant_size_v< Value > >( Transaction & t, const Value & value )
        {
            throw_btree_error( false, RetCode::InvalidData, "Unable to resolve type index" );
            return PackedValue( std::variant_npos, 0 );
        }


        /* Unpacks value

        @param [in] t - active transaction
        @retval unpacked - value
        @throw btree_error, storage_file_serror
        */
        template < size_t I >
        Value unpack( StorageFile & f ) const
        {
            using namespace std;

            if ( I == type_index_ )
            {
                using value_type = variant_alternative_t< I, Value >;

                if constexpr ( is_blob_type< value_type >::value )
                {
                    using StreamCharT = typename is_blob_type< value_type >::StreamCharT;
                    
                    value_type value;
                    
                    auto buffer = f.get_chain_reader< StreamCharT >( value_ );
                    std::basic_istream< StreamCharT > is( &buffer );
                    is >> value;

                    return is_move_constructible_v< value_type > ? Value{ move( value ) } : Value{ value };
                }
                else
                {
                    value_type value;

                    copy(
                        reinterpret_cast< const char* >( &value_ ),
                        reinterpret_cast< const char* >( &value_ ) + sizeof( value ),
                        reinterpret_cast< char* >( &value )
                    );

                    return is_move_constructible_v< value_type > ? Value{ move( value ) } : Value{ value };
                }
            }
            else
            {
                return unpack< I + 1 >( f );
            }
        }


        //
        // terminal specialization of unpack<>()
        //
        template <>
        Value unpack< std::variant_size_v< Value > >( StorageFile & ) const
        {
            throw_btree_error( false, RetCode::InvalidData, "Unable to resolve type index" );
            return Value{};
        }


        /* Expilcit consrutor, creates an assigned instance

        @param [in] type_index - index of assigned type
        @param [in] value - value packed into uint64_t or UID of BLOB object
        @throw nothing
        */
        explicit PackedValue( size_t type_index, uint64_t value ) noexcept : type_index_( type_index ), value_( value ) {}


    public:

        /** Default constructor, creates dummy object

        @throw nothing
        */
        PackedValue() noexcept : type_index_( std::variant_npos ) {}


        /** Class is copyable...
        */
        PackedValue( const PackedValue & ) noexcept = default;
        PackedValue( PackedValue&& ) noexcept = default;


        /** ...and movable
        */
        PackedValue & operator = ( const PackedValue & v ) noexcept = default;
        PackedValue & operator = ( PackedValue&& ) noexcept = default;


        /** Let's know if assigned value stored as BLOB
        */
        auto is_blob() const
        {
            return check_for_blob< 0 >();
        }


        /** Converts value into packed representation

        @param [in] t - transaction to be used to store BLOB
        @param [in] value - value to be packed
        @retval packed value
        @throw storage_file_error
        */
        static PackedValue make_packed( Transaction & t, const Value & value )
        {
            return pack< 0 >( t, value );
        }


        /** Unpack value

        @param [in] f - file to be used
        @retval unpacked value
        @throw storage_file_error
        */
        Value unpack( StorageFile & f ) const
        {
            return unpack< 0 >( f );
        }

        
        /** Erases associated BLOB

        @param [in] t - transaction
        @throw storage_file_error
        */
        void erase_blob( Transaction & t ) const
        {
            if ( check_for_blob< 0 >() )
            {
                t.erase_chain( value_ );
            }
        }


        /** Output streaming operator

        @param [in/out] os - output stream
        @param [in] - value to be streamed out
        @retval std::ostream - updated output stream
        @throw btree_error, storage_file_error
        */
        friend std::ostream & operator << ( std::ostream & os, const PackedValue & v )
        {
            big_uint64_t type_index = v.type_index_;
            big_uint64_t value = v.value_;

            os.write( reinterpret_cast< const char* >( &type_index ), sizeof( type_index ) );
            throw_btree_error( os.good(), RetCode::UnknownError );

            os.write( reinterpret_cast< const char* >( &value ), sizeof( value ) );
            throw_btree_error( os.good(), RetCode::UnknownError );

            return os;
        }


        /** Input streaming operator

        @param [in/out] is - input stream
        @param [in] - value to be streamed in
        @retval std::istream - updated input stream
        @throw btree_error, storage_file_error
        */
        friend std::istream & operator >> ( std::istream & is, PackedValue & v )
        {
            big_uint64_t type_index;
            big_uint64_t value;

            is.read( reinterpret_cast< char* >( &type_index ), sizeof( type_index ) );
            throw_btree_error( is.good(), RetCode::UnknownError );

            is.read( reinterpret_cast< char* >( &value ), sizeof( value ) );
            throw_btree_error( is.good(), RetCode::UnknownError );

            v.type_index_ = static_cast< size_t >( type_index );
            v.value_ = value;

            return is;
        }
    };
}

#endif
