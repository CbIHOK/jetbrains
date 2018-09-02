#ifndef __JB__PACKED_VALUE__H__
#define __JB__PACKED_VALUE__H__


#include <type_traits>


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

    template < typename T >
    struct is_blob_type
    {
        static constexpr bool value = ! std::is_integral_v< T >;
        using StreamCharT = char;
    };

    template <>
    struct is_blob_type< float >
    {
        static constexpr bool value = false;
        using StreamCharT = char;
    };

    template <>
    struct is_blob_type< double >
    {
        static constexpr bool value = false;
        using StreamCharT = char;
    };

    template <>
    struct is_blob_type< std::string >
    {
        static constexpr bool value = true;
        using StreamCharT = char;
    };

    template <>
    struct is_blob_type< std::wstring >
    {
        static constexpr bool value = true;
        using StreamCharT = wchar_t;
    };

    template <>
    struct is_blob_type< std::basic_string< char16_t > >
    {
        static constexpr bool value = true;
        using StreamCharT = char16_t;
    };

    template <>
    struct is_blob_type< std::basic_string< char32_t > >
    {
        static constexpr bool value = true;
        using StreamCharT = char32_t;
    };


    template < typename Policies >
    struct Storage< Policies >::PhysicalVolumeImpl::BTree::PackedValue
    {
        friend class TestPackedValue;

        using Value = typename Storage::Value;
        using big_uint64_t = boost::endian::big_uint64_at;

        size_t type_index_;
        uint64_t value_;

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


        template < size_t I >
        static PackedValue serialize( Transaction & t, const Value & value )
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
                return serialize< I + 1 >( t, value );
            }
        }

        template <>
        static PackedValue serialize< std::variant_size_v< Value > >( Transaction & t, const Value & value )
        {
            throw_btree_error( false, RetCode::InvalidData, "Unable to resolve type index" );
            return PackedValue( std::variant_npos, 0 );
        }

        template < size_t I > 
        Value deserialize_value( StorageFile & f ) const
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
                    value_type v_;

                    copy(
                        reinterpret_cast< const char* >( &value_ ),
                        reinterpret_cast< const char* >( &value_ ) + sizeof( v_ ),
                        reinterpret_cast< char* >( &v_ )
                    );

                    return Value{ v_ };
                }
            }
            else
            {
                return deserialize_value< I + 1 >( f );
            }
        }

        template <>
        Value deserialize_value< std::variant_size_v< Value > >( StorageFile & ) const
        {
            throw_btree_error( false, RetCode::InvalidData, "Unable to resolve type index" );

            return Value{};
        }

        explicit PackedValue( size_t type_index, uint64_t value ) : type_index_( type_index ), value_( value ) {}

    public:

        PackedValue() : type_index_( std::variant_npos ) {}

        PackedValue( const PackedValue & ) noexcept = default;
        PackedValue( PackedValue&& ) noexcept = default;

        PackedValue & operator = ( const PackedValue & v ) noexcept = default;
        PackedValue & operator = ( PackedValue&& ) noexcept = default;

        auto is_blob() const
        {
            return check_for_blob< 0 >();
        }

        /** 
        */
        static PackedValue make_packed( Transaction & t, const Value & value )
        {
            return serialize< 0 >( t, value );
            //return PackedValue( std::variant_npos, 0 );;
        }


        Value unpack( StorageFile & f ) const
        {
            return deserialize_value< 0 >( f );
        }

        
        void erase_blob( Transaction & t ) const
        {
            if ( check_for_blob< 0 >() )
            {
                t.erase_chain( value_ );
            }
        }

        friend std::ostream & operator << ( std::ostream & os, const PackedValue & v )
        {
            big_uint64_t type_index = v.type_index_;
            big_uint64_t value = v.value_;

            os.write( reinterpret_cast< const char* >( &type_index ), sizeof( type_index ) );
            os.write( reinterpret_cast< const char* >( &value ), sizeof( value ) );

            return os;
        }

        friend std::istream & operator >> ( std::istream & is, PackedValue & v )
        {
            big_uint64_t type_index;
            big_uint64_t value;

            is.read( reinterpret_cast< char* >( &type_index ), sizeof( type_index ) );
            is.read( reinterpret_cast< char* >( &value ), sizeof( value ) );

            v.type_index_ = static_cast< size_t >( type_index );
            v.value_ = value;

            return is;
        }
    };
}

#endif
