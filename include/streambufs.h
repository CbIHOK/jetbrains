#ifndef __JB__ISTREAMBUF__H__
#define __JB__ISTREAMBUF__H__


#include <streambuf>
#include <limits>
#include <type_traits>

#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


namespace jb
{
    /** Reflects char type -> integer type
    */
    template < typename CharT > struct store_adaptor_type
    {
        using type = typename std::char_traits< CharT >::int_type;
    };


    /** Specialization for CharT=char
    */
    template <> struct store_adaptor_type< char >
    {
        using type = char;
    };


    /** Short alias for store_adaptor_type<>::type
    */
    template < typename CharT > using store_adaptor_type_t = typename store_adaptor_type< CharT >::type;


    /* Reflects character type -> stored type
    */
    template < typename CharT > struct stored_type
    {
        using be_type = boost::endian::endian_arithmetic<
            boost::endian::order::big,
            store_adaptor_type_t< CharT >,
            sizeof( CharT ) * 8,
            boost::endian::align::yes
        >;

        static constexpr char c[ sizeof( int ) ] = { 1 };
        static constexpr int i = { 1 };

        using type = std::conditional_t< i == c[ 0 ], be_type, CharT >;
    };

    /** Specialization for CharT=char
    */
    template <> struct stored_type< char > { using type = char; };


    /* Short alias for stored_type<>::type
    */
    template < typename CharT > using stored_type_t = typename stored_type< CharT >::type;


    /** Represents input stream from storage file

    @tparam Policies - global setting
    */
    template < typename Policies >
    template < typename CharT >
    class Storage< Policies >::PhysicalVolumeImpl::StorageFile::istreambuf : public std::basic_streambuf< CharT >
    {
        //
        // requires access to private explicit constructor
        //
        friend class StorageFile;


        //
        // stored type for CharT
        //
        using StoredType = stored_type_t< CharT >;
        using AdaptorType = store_adaptor_type_t< CharT >;
        //using traits_type = typename streambuf_traits< CharT >::traits_type;
        using traits_type = std::char_traits< CharT >;
        using int_type = typename traits_type::int_type;


        //
        // buffer size
        //
        static constexpr auto BufferSize = ChunkOffsets::sz_Space / sizeof( StoredType );
        using buffer_t = std::array< CharT, BufferSize >;


        //
        // data members
        //
        StorageFile & file_;
        streamer_t reader_;
        ChunkUid current_chunk_ = InvalidChunkUid;
        Handle & handle_;
        buffer_t & buffer_;


        /* Exlplicit constructor ( only StorageFile can instanciate this class )

        @param [in] file - associated storage file
        @param [in] handle - associated handle
        @param [in] start_chunk - start chunk of the chain to be read
        @throw nothing
        */
        explicit istreambuf( StorageFile & file, streamer_t && reader, ChunkUid start_chunk ) noexcept
            : file_( file )
            , reader_( move( reader ) )
            , handle_( reader_.first )
            , buffer_( reinterpret_cast< buffer_t& >( reader_.second.get() ) )
            , current_chunk_( start_chunk )
        {
            // initialize pointer like all data is currently read-out
            auto start = buffer_.data();
            auto end = start + BufferSize;
            setg( start, end, end );
        }

        //
        // handles lack of data
        //
        virtual int_type underflow() override
        {
            using namespace std;

            // we ain't ok
            if ( RetCode::Ok != file_.status() )
            {
                return traits_type::eof();
            }

            // we still have something to get
            if ( gptr() < egptr() )
            {
                return *gptr();
            }

            if ( InvalidChunkUid == current_chunk_ )
            {
                return traits_type::eof();
            }

            // nubmer of available elements
            size_t read_chars = 0;

            // if CharT is stored type
            if constexpr ( is_same_v< CharT, StoredType > )
            {
                // read data from current chunk
                auto[ read_bytes, next_chunk ] = file_.read_chunk(
                        reader_.first,
                        current_chunk_,
                        reinterpret_cast< void* >( buffer_.data() ),
                        BufferSize * sizeof( CharT ) );

                if ( read_bytes )
                {
                    throw_storage_file_error( read_bytes % sizeof( StoredType ) == 0, RetCode::IoError, "Invalid amount of data" );

                    // caclculate number of read elements
                    read_chars = read_bytes / sizeof( CharT );
                    throw_storage_file_error( read_chars <= BufferSize, RetCode::IoError, "Buffer overflow" );

                    // proceed to the next chunk
                    current_chunk_ = next_chunk;
                }
            }
            else
            {
                // read from file to intermidiate array of stored type
                std::array< StoredType, BufferSize > typed_adaptor;

                // read data from current chunk
                auto[ read_bytes, next_chunk ] = file_.read_chunk(
                    reader_.first,
                    current_chunk_,
                    reinterpret_cast< void* >( typed_adaptor.data() ),
                    typed_adaptor.size() * sizeof( StoredType ) );

                if ( read_bytes )
                {
                    throw_storage_file_error( read_bytes % sizeof( StoredType ) == 0, RetCode::UnknownError, "Invalid data size" );

                    // caclculate number of read elements
                    read_chars = read_bytes / sizeof( StoredType );
                    throw_storage_file_error( read_chars <= BufferSize, RetCode::UnknownError, "Buffer overflow" );

                    // proceed to the next chunk
                    current_chunk_ = next_chunk;

                    // convert elements to platform specific representation
                    for ( size_t i = 0; i < read_chars; ++i )
                    {
                        buffer_[ i ] = static_cast< AdaptorType >( typed_adaptor[ i ] );
                    }
                }
            }

            // set pointers
            setg( buffer_.data(), buffer_.data(), buffer_.data() + read_chars );

            // return char if available
            if ( read_chars > 0 )
            {
                return *gptr();
            }
            else
            {
                return traits_type::eof();
            }
        }


    public:

        /** The class is not default creatable/copyable
        */
        istreambuf() = delete;
        istreambuf( const istreambuf & ) = delete;
        istreambuf & operator = ( const istreambuf & ) = delete;


        /** ...but movable
        */
        istreambuf( istreambuf&& ) = default;
        istreambuf & operator = ( istreambuf&& ) = default;


        /** Destructor, releases allocated handle

        @throw nothing
        @note in theory the body may fire an exception but here it's much better to die on noexcept
              guard than to analyze a deadlock
        */
        ~istreambuf() noexcept
        {
            std::scoped_lock l( file_.readers_mutex_ );
            file_.reader_stack_.push( reader_ );
            file_.readers_cv_.notify_one();
        }
    };


    /** Represents output stream to storage file

    @tparam Policies - global setting
    */
    template < typename Policies >
    template < typename CharT >
    class Storage< Policies >::PhysicalVolumeImpl::StorageFile::ostreambuf : public std::basic_streambuf< CharT >
    {
        friend class Transaction;

        using StoredType = stored_type_t< CharT >;
        using AdaptorType = store_adaptor_type_t< CharT >;
        //using traits_type = typename streambuf_traits< CharT >::traits_type;
        using traits_type = std::char_traits< CharT >;
        using int_type = typename traits_type::int_type;

        static constexpr auto BufferSize = ChunkOffsets::sz_Space / sizeof( StoredType );
        using buffer_t = std::array< CharT, BufferSize >;

        Transaction & transaction_;
        streamer_t streamer_;
        buffer_t & buffer_;

        //
        // Explicit constructor available only for Transaction
        //
        explicit ostreambuf( Transaction & transaction, streamer_t & streamer ) noexcept
            : transaction_( transaction )
            , streamer_( streamer )
            , buffer_( reinterpret_cast< buffer_t& >( streamer_.second.get() ) )
        {
            auto const start = buffer_.data();
            auto const end = start + BufferSize;
            setp( start, end - 1 );
        }

    protected:

        //
        // buffer overflow handler
        //
        virtual int_type overflow( int_type c ) override
        {
            if ( c != traits_type::eof() )
            {
                *pptr() = c;
                pbump( 1 );
                return sync() == 0 ? c : traits_type::eof();
            }
            return traits_type::eof();
        }

        //
        // sends buffer to transaction
        //
        virtual int sync() override
        {
            using namespace std;

            // nothing to write
            if ( pptr() == pbase() )
            {
                return 0;
            }

            assert( pptr() - pbase() > 0 );
            size_t elements_to_write = static_cast< size_t >( pptr() - pbase() );
            size_t elements_written = 0;

            if constexpr ( is_same_v< CharT, StoredType > )
            {
                auto bytes_written = transaction_.write( buffer_.data(), elements_to_write * sizeof( CharT ) );

                if ( bytes_written )
                {
                    throw_storage_file_error( bytes_written % sizeof( CharT ) == 0, RetCode::UnknownError, "Invalid data size" );
                    elements_written = bytes_written / sizeof( CharT );
                }
            }
            else
            {
                std::array< StoredType, BufferSize > typed_adaptor;

                for ( size_t i = 0 ; i < elements_to_write; ++i )
                {
                    typed_adaptor[ i ] = static_cast< AdaptorType >( buffer_[ i ] );
                }

                auto bytes_written = transaction_.write( typed_adaptor.data(), elements_to_write * sizeof( StoredType ) );

                if ( bytes_written )
                {
                    throw_storage_file_error( bytes_written % sizeof( CharT ) == 0, RetCode::UnknownError, "Invalid data size" );
                    elements_written = bytes_written / sizeof( StoredType );
                }
            }

            if ( elements_written )
            {
                pbump( -static_cast< int >( elements_written ) );
                return 0;
            }
            {
                return -1;
            }
        }


    public:

        /** Default constructor, creates dummy buffer
        */
        ostreambuf() = delete;


        /* The class is not copyable...
        */
        ostreambuf( const ostreambuf & ) = delete;
        ostreambuf & operator = ( const ostreambuf & ) = delete;


        /** ...but movable
        */
        ostreambuf( ostreambuf&& ) = default;
        ostreambuf & operator = ( ostreambuf&& ) = default;
    };

}

#endif
