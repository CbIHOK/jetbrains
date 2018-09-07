#ifndef __JB__STORAGE_FILE__H__
#define __JB__STORAGE_FILE__H__


#include <tuple>
#include <limits>
#include <filesystem>
#include <string>
#include <array>
#include <execution>
#include <stack>
#include <mutex>
#include <condition_variable>
#include <streambuf>

#include <boost/container/static_vector.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/archive/text_oarchive.hpp>

#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


namespace jb
{
    /** Implements physical storage

    @tparam Policies - global settings
    */
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl::StorageFile
    {
        template < typename T > friend class TestStorageFile;

        using RetCode = Storage::RetCode;
        using KeyCharT = typename Policies::KeyCharT;
        using ValueT = typename Storage::Value;
        using Os = typename Policies::Os;
        using Handle = typename Os::HandleT;
        using big_uint32_t = boost::endian::big_int32_at;
        using big_uint64_t = boost::endian::big_uint64_at;
        template < typename T, size_t C > using static_vector = boost::container::static_vector< T, C >;

        inline static const Handle InvalidHandle = Os::InvalidHandle;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;
        static constexpr auto ReaderNumber = Policies::PhysicalVolumePolicy::ReaderNumber;
        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static constexpr auto ChunkSize = Policies::PhysicalVolumePolicy::ChunkSize;

        using io_buffer_t = std::array< char, ChunkSize >;
        using streamer_t = std::pair < Handle, std::reference_wrapper< io_buffer_t > >;


    public:

        // declares chunk uid
        using ChunkUid = uint64_t;
        static constexpr ChunkUid InvalidChunkUid = std::numeric_limits< ChunkUid >::max() - 1;


        //
        // few forwarding declarations
        //
        class Transaction;
        template < typename CharT > class ostreambuf;
        template < typename CharT > class istreambuf;


    private:

        //
        // needs access to private commit() & rollback()
        //
        friend class Transaction;


        //
        // needs access to private read_chunk()
        //
        template < typename CharT > class istreambuf;


        // status
        RetCode status_ = RetCode::Ok;
        bool newly_created_ = false;

        // file locking
        std::string file_lock_name_;
        std::shared_ptr< boost::interprocess::named_mutex > file_lock_;

        // writing objects
        mutable std::mutex write_mutex_;
        io_buffer_t write_buffer_;
        streamer_t writer_;

        // bloom writer
        Handle bloom_ = InvalidHandle;

        // readers
        std::mutex readers_mutex_;
        std::condition_variable readers_cv_;
        std::array< io_buffer_t, ReaderNumber > read_buffers_;
        using reader_stack_t = std::stack< streamer_t, static_vector< streamer_t, ReaderNumber > >;
        reader_stack_t reader_stack_;


        //
        // defines chunk structure
        //
        struct chunk_t
        {
            uint8_t head_;
            uint8_t released_;
            uint8_t reserved_1_;
            uint8_t reserved_2_;
            big_uint32_t used_size_;                   //< number of utilized bytes in chunk
            big_uint64_t next_used_;                   //< next used chunk (takes sense for allocated chunks)
            big_uint64_t next_free_;                   //< next free chunk (takes sense for released chunks)
            std::array< int8_t, ChunkSize > space_;    //< available space
        };


        //
        // defines offsets and sizes of chunk fields
        //
        enum ChunkOffsets
        {
            of_UsedSize = offsetof( chunk_t, used_size_ ),
            sz_UsedSize = sizeof( chunk_t::used_size_ ),

            of_NextUsed = offsetof( chunk_t, next_used_ ),
            sz_NextUsed = sizeof( chunk_t::next_used_ ),

            of_NextFree = offsetof( chunk_t, next_free_ ),
            sz_NextFree = sizeof( chunk_t::next_free_ ),

            of_Space = offsetof( chunk_t, space_ ),
            sz_Space = sizeof( chunk_t::space_ ),
        };


        //
        // defines header structure
        //
        struct header_t
        {
            big_uint64_t compatibility_stamp;         //< software compatibility stamp

            uint8_t bloom_[ BloomSize ];              //< bloom filter data

            struct transactional_data_t
            {
                big_uint64_t file_size_;              //< current file size
                big_uint64_t free_space_;             //< pointer to first free chunk (garbage collector)
            };

            transactional_data_t transactional_data_; //< original copy
            transactional_data_t transaction_;        //< transaction copy
            big_uint64_t transaction_crc_;            //< transaction CRC (identifies valid transaction)

            struct preserved_chunk_t
            {
                big_uint64_t target_;                 //< target chunk uid
                chunk_t chunk_;                       //< preserved chunk
            };
            preserved_chunk_t overwritten__chunk_;    //< let us make one writing per transaction with preservation of original chunk uid
        };


        //
        // defines transaction data offsets/sizes
        //
        enum TransactionDataOffsets
        {
            of_FileSize = offsetof( header_t::transactional_data_t, file_size_ ),
            sz_FileSize = sizeof( header_t::transactional_data_t::file_size_ ),

            of_FreeSpace = offsetof( header_t::transactional_data_t, free_space_ ),
            sz_FreeSpace = sizeof( header_t::transactional_data_t::free_space_ ),
        };


        //
        // defines preserved chunk structure offsets/chunks
        //
        enum PreservedChunkOffsets
        {
            of_Target = offsetof( header_t::preserved_chunk_t, target_ ),
            sz_Target = sizeof( header_t::preserved_chunk_t::target_ ),

            of_Chunk = offsetof( header_t::preserved_chunk_t, chunk_ ),
            sz_Chunk = sizeof( header_t::preserved_chunk_t::chunk_ )
        };


        //
        // defines header structure offsets/sizes
        //
        enum HeaderOffsets
        {
            of_CompatibilityStamp = offsetof( header_t, compatibility_stamp ),
            sz_CompatibilityStamp = sizeof( header_t::compatibility_stamp ),

            of_Bloom = offsetof( header_t, bloom_ ),
            sz_Bloom = sizeof( header_t::bloom_ ),

            of_TransactionalData = offsetof( header_t, transactional_data_ ),
            sz_TransactionalData = sizeof( header_t::transactional_data_ ),

            of_Transaction = offsetof( header_t, transaction_ ),
            sz_Transaction = sizeof( header_t::transaction_ ),

            of_TransactionCrc = offsetof( header_t, transaction_crc_ ),
            sz_TransactionCrc = sizeof( header_t::transaction_crc_ ),

            of_PreservedChunk = offsetof( header_t, overwritten__chunk_ ),
            sz_PreservedChunk = sizeof( header_t::overwritten__chunk_ ),

            of_Root = sizeof( header_t )
        };


    public:

        //
        // declares uid of the Root chunk
        //
        static constexpr ChunkUid RootChunkUid = static_cast< ChunkUid >( HeaderOffsets::of_Root );

        class storage_file_error : public std::runtime_error
        {
            RetCode rc_;

        public:
            storage_file_error( RetCode rc, const char * what ) : std::runtime_error( what ), rc_( rc ) {}
            RetCode code() const { return rc_; }
        };


    private:


        /* Sets object status

        @param [in] status - status to be set
        @throw nothing
        */
        //auto set_status( RetCode status ) noexcept
        //{
        //    auto ok = RetCode::Ok;
        //    status_.compare_exchange_weak( ok, status, std::memory_order_acq_rel, std::memory_order_relaxed );
        //}


        /* Throws storage_file_error exception if given condition failed

        @param [in] condition - condition to be checked
        @param [in] rc - return code to be assigned to an exception
        @param [in] what - text message to be assigned to an exception
        @throw storage_file_error
        */
        static auto throw_storage_file_error( bool condition, RetCode rc, const char * what = "" )
        {
            if ( !condition ) throw storage_file_error( rc, what );
        }


        /* If a condition failed throws std::logic_error with given text message an immediately die

        on noexcept guard calling terminate() handler. That gives the ability to collect crash dump

        @param [in] condition - condition to be checked
        @param [in] what - text message to be fired
        @throw nothing
        */
        static auto throw_logic_error( bool condition, const char * what ) noexcept
        {
            if ( !condition ) throw std::logic_error( what );
        }


        /* Generates compatibility stamp basing on the system policies

        @retval unique stamp of software settings
        @throw nothing
        */
        static uint64_t generate_compatibility_stamp() noexcept
        {
            using namespace std;
            auto hash = variadic_hash( type_index( typeid( Key ) ), type_index( typeid( ValueT ) ), BloomSize, MaxTreeDepth, BTreeMinPower, ChunkSize );
            return hash;
        }


        /* Check software to file compatibility

        @throws storage_file_error
        @note function does not imply concurrent execution
        */
        auto check_compatibility()
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file object" );

            Handle handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            big_uint64_t stamp;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_CompatibilityStamp );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_CompatibilityStamp, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &stamp, sizeof( stamp ) );
                throw_storage_file_error( ok && read == sizeof( stamp ), RetCode::IoError );
            }

            throw_storage_file_error( stamp == generate_compatibility_stamp(), RetCode::IncompatibleFile );
        }


        /* Initialize newly create file with valid header

        @throw storage_file_error
        @note function does not imply concurrent execution
        */
        auto deploy()
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file object" );

            Handle handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // reserve space for header
            {
                auto[ ok, size ] = Os::resize_file( handle, sizeof( header_t ) );
                throw_storage_file_error( ok && size == sizeof( header_t ), RetCode::IoError );
            }

            // write compatibility stamp
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_CompatibilityStamp );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_CompatibilityStamp, RetCode::IoError );
            }
            {
                boost::endian::big_uint64_t stamp = generate_compatibility_stamp();

                auto[ ok, written ] = Os::write_file( handle, &stamp, sizeof( stamp ) );
                throw_storage_file_error( ok && written == sizeof( stamp ), RetCode::IoError );
            }

            // write file size
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize, RetCode::IoError );
            }
            {
                big_uint64_t file_size = HeaderOffsets::of_Root;

                auto[ ok, written ] = Os::write_file( handle, &file_size, sizeof( file_size ) );
                throw_storage_file_error( ok && written == sizeof( file_size ), RetCode::IoError );
            }

            // invalidate free space ptr
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace, RetCode::IoError );
            }
            {
                big_uint64_t free_space = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( handle, &free_space, sizeof( free_space ) );
                throw_storage_file_error( ok && written == sizeof( free_space ), RetCode::IoError );
            }

            // invalidate transaction
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionCrc );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionCrc, RetCode::IoError );
            }
            {
                boost::endian::big_uint64_t invalid_crc = variadic_hash( HeaderOffsets::of_Root, InvalidChunkUid ) + 1;

                auto[ ok, written ] = Os::write_file( handle, &invalid_crc, sizeof( invalid_crc ) );
                throw_storage_file_error( ok && written == sizeof( invalid_crc ), RetCode::IoError );
            }
        }


        /* Commit transaction

        Applies all the changes that have been done during last successful transaction

        @retval operation status
        @throw storage_file_error
        */
        auto commit()
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file object" );

            Handle handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // read transaction data
            header_t::transactional_data_t transaction;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_Transaction );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_Transaction, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &transaction, sizeof( transaction ) );
                throw_storage_file_error( ok && read == sizeof( transaction ), RetCode::IoError );
            }

            // read CRC
            boost::endian::big_uint64_t transaction_crc;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionCrc );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionCrc, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &transaction_crc, sizeof( transaction_crc ) );
                throw_storage_file_error( ok && read == sizeof( transaction_crc ), RetCode::IoError );
            }

            // validate transaction
            uint64_t file_size = transaction.file_size_, free_space = transaction.free_space_;
            auto valid_transaction = ( transaction_crc == variadic_hash( file_size, free_space ) );

            // if we have valid transaction
            if ( valid_transaction )
            {
                // read preserved chunk target
                big_uint64_t preserved_target;
                {
                    auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                    throw_storage_file_error( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target, RetCode::IoError );
                }

                {
                    auto[ ok, read ] = Os::read_file( handle, &preserved_target, sizeof( preserved_target ) );
                    throw_storage_file_error( ok && read == sizeof( preserved_target ), RetCode::IoError );
                }

                // if there is preserved chunk
                if ( preserved_target != InvalidChunkUid )
                {
                    // read preserved chunk
                    chunk_t preserved_chunk;
                    {
                        auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Chunk );
                        throw_storage_file_error( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Chunk, RetCode::IoError );
                    }
                    {
                        auto[ ok, read ] = Os::read_file( handle, &preserved_chunk, sizeof( preserved_chunk ) );
                        throw_storage_file_error( ok && read == sizeof( preserved_chunk ), RetCode::IoError );
                    }

                    // copy preserved chunk to target
                    {
                        auto[ ok, pos ] = Os::seek_file( handle, preserved_target );
                        throw_storage_file_error( ok && pos == preserved_target, RetCode::IoError );
                    }
                    {
                        auto[ ok, written ] = Os::write_file( handle, &preserved_chunk, sizeof( preserved_chunk ) );
                        throw_storage_file_error( ok && written == sizeof( preserved_chunk ), RetCode::IoError );
                    }
                }

                // apply transaction
                {
                    auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData );
                    throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData, RetCode::IoError );
                }
                {
                    auto[ ok, written ] = Os::write_file( handle, &transaction, sizeof( transaction ) );
                    throw_storage_file_error( ok && written == sizeof( transaction ), RetCode::IoError );
                }

                // invalidate transaction
                {
                    auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionCrc );
                    throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionCrc, RetCode::IoError );
                }
                {
                    uint64_t file_size = transaction.file_size_, free_space = transaction.free_space_;
                    boost::endian::big_uint64_t invalid_crc = variadic_hash( file_size, free_space ) + 1;

                    auto[ ok, written ] = Os::write_file( handle, &invalid_crc, sizeof( invalid_crc ) );
                    throw_storage_file_error( ok && written == sizeof( invalid_crc ), RetCode::IoError );
                }
            }
            else
            {
                rollback();
            }
        }


        /* Rollback transaction

        Revert all the changes that has been done to the file since start of transaction (except
        Bloom filter data). If an error occures the function throw an exception and immediately die
        on noexcept guard calling terminate() handler. That gives the ability to collect crash dump

        @throw nothing
        */
        auto rollback() noexcept
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file object" );

            Handle handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // just restore file size
            big_uint64_t file_size;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &file_size, sizeof( file_size ) );
                throw_storage_file_error( ok && read == sizeof( file_size ), RetCode::IoError );
            }
            {
                auto[ ok, size ] = Os::resize_file( handle, file_size );
                throw_storage_file_error( ok && size == file_size, RetCode::IoError );
            }
        }


        /* Reads another chunk of a chain

        @param [in] handle - file handle to be used
        @param [in] chunk - uid of chunk to be read
        @param [out] buffer - i/o buffer
        @param [in] buffer_size - maximum abount of bytes to be read
        @retval size_t - number of read bytes
        @retval ChunkUid - the next chunk in the chain
        @throw storage_file_error
        */
        [[ nodiscard ]]
        std::tuple< size_t, ChunkUid > read_chunk( Handle handle, ChunkUid chunk, void * buffer, size_t buffer_size )
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file object" );
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );
            throw_logic_error( InvalidChunkUid != chunk && chunk >= HeaderOffsets::of_Root, "Invalid chunk" );
            throw_logic_error( buffer && buffer_size, "Invalid read buffer" );

            // get next used
            big_uint64_t next_used;
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk + ChunkOffsets::of_NextUsed );
                throw_storage_file_error( ok && pos == chunk + ChunkOffsets::of_NextUsed, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &next_used, sizeof( next_used ) );
                throw_storage_file_error( ok && read == sizeof( next_used ), RetCode::IoError );
            }

            // get size of utilized space in chunk
            big_uint32_t used_size;
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk + ChunkOffsets::of_UsedSize );
                throw_storage_file_error( ok && pos == chunk + ChunkOffsets::of_UsedSize, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &used_size, sizeof( used_size ) );
                throw_storage_file_error( ok && read == sizeof( used_size ), RetCode::IoError );
            }

            // read size
            auto size_to_read = std::min( buffer_size, static_cast< size_t >( used_size ) );

            // read data
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk + ChunkOffsets::of_Space );
                throw_storage_file_error( ok && pos == chunk + ChunkOffsets::of_Space, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, buffer, size_to_read );
                throw_storage_file_error( ok && read == size_to_read, RetCode::IoError );
            }

            return { size_to_read, next_used };
        }


    public:

        /** The class is not default creatable/copyable/movable
        */
        StorageFile() = delete;
        StorageFile( StorageFile && ) = delete;


        /** Constructs an instance

        @param [in] path - path to physical file@param [in] suppress_lock - do not lock the file (test mode)
        @throw nothing
        */
        explicit StorageFile( const std::filesystem::path & path, bool suppress_lock = false ) noexcept try
            : file_lock_name_( "jb_lock_" + std::to_string( std::filesystem::hash_value( path ) ) )
            , writer_( InvalidHandle, std::ref( write_buffer_ ) )
        {
            using namespace std;

            //
            // Unfortunately MS does not care about standards as usual and HANDLED exceptions easily leaves
            // try-catch constructors by an rethrow. That is why I have to use such workaround
            //
            if ( !suppress_lock ) try
            {
                // lock file
                file_lock_ = std::make_shared< boost::interprocess::named_mutex >( boost::interprocess::create_only_t{}, file_lock_name_.c_str() );
            }
            catch ( const boost::interprocess::interprocess_exception & )
            {
                status_ = RetCode::AlreadyOpened;
                return;
            }

            // open writter
            {
                auto[ opened, tried_create, handle ] = Os::open_file( path );

                newly_created_ = tried_create;
                writer_.first = handle;

                throw_storage_file_error( opened, RetCode::UnableToOpen );
            }

            // deploy new file
            if ( newly_created_ ) deploy();

            // check compatibility for existing file
            if ( !newly_created_ ) try
            {
                check_compatibility();
            }
            catch ( const storage_file_error & e )
            {
                status_ = e.code();
                return;
            }

            // apply valid transaction if exists
            if ( !newly_created_ ) commit();

            // open Bloom writer/reader
            {
                auto[ opened, tried_create, handle ] = Os::open_file( path );
                throw_storage_file_error( ( bloom_ = handle ) != InvalidHandle, RetCode::UnableToOpen );
            }

            // open readers
            for ( size_t i = 0; i < ReaderNumber; ++i )
            {
                auto[ opened, tried_create, handle ] = Os::open_file( path );
                throw_storage_file_error( InvalidHandle != handle, RetCode::UnableToOpen );

                reader_stack_.push( streamer_t{ handle, ref( read_buffers_[ i ] ) } );
            }
        }
        catch ( const storage_file_error & e )
        {
            status_ = e.code();
        }
        catch ( const std::bad_alloc & )
        {
            status_ = RetCode::InsufficientMemory;
        }


        /** Destructor
        
        releases allocated resources

        @throw nothing
        */
        ~StorageFile()
        {
            using namespace std;

            // release writer
            if ( writer_.first != InvalidHandle ) Os::close_file( writer_.first );

            // release bloom data writer
            if ( bloom_ != InvalidHandle ) Os::close_file( bloom_ );

            // release readers
            while ( reader_stack_.size() )
            {
                streamer_t & reader = reader_stack_.top();
                if ( reader.first != InvalidHandle ) Os::close_file( reader.first );
                reader_stack_.pop();
            }

            // unlock file name
            if ( file_lock_ )
            {
                file_lock_.reset();
                boost::interprocess::named_mutex::remove( file_lock_name_.c_str() );
            }
        }


        /** Provides creation status

        @retval RetCode - object status
        @throw nothing
        */
        [[nodiscard]]
        auto status() const noexcept { return status_; }


        /** Let's know if associated file has been just created

        @retval bool - true if this is new file
        @throw nothing
        */
        [[nodiscard]]
        auto newly_created() const noexcept { return newly_created_; }


        /** Reads data for Bloom filter

        @param [out] bloom_buffer - target buffer for Bloom data
        @throw storage_file_error
        @note the function does not imply concurrent calls
        */
        auto read_bloom( uint8_t * bloom_buffer )
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file" );

            if ( !newly_created_ )
            {
                throw_logic_error( InvalidHandle != bloom_, "Invalid handle" );

                // seek & read data
                {
                    auto[ ok, pos ] = Os::seek_file( bloom_, HeaderOffsets::of_Bloom );
                    throw_storage_file_error( ok && pos == HeaderOffsets::of_Bloom, RetCode::IoError );
                }
                {
                    auto[ ok, read ] = Os::read_file( bloom_, bloom_buffer, BloomSize );
                    throw_storage_file_error( ok && read == BloomSize, RetCode::IoError );
                }
            }
            else
            {
                assert( reinterpret_cast< size_t >( bloom_buffer ) % sizeof( uint64_t ) == 0 && BloomSize % sizeof( uint64_t ) == 0 );

                // STOSB -> STOSQ
                const auto start = reinterpret_cast< uint64_t* >( bloom_buffer );
                const auto end = start + BloomSize / sizeof( uint64_t );
                std::fill( start, end, 0 );
            }
        }


        /** Write another digest to Bloom filter section

        @param [in] byte_no - ordinal number of byte in the array
        @param [in] byte - value to be written
        @thrown storage_file_error
        @note the function is not thread safe, but it's guaranied by the caller
        */
        auto add_bloom_digest( size_t byte_no, uint8_t byte )
        {
            throw_logic_error( RetCode::Ok == status_, "Invalid file" );
            throw_logic_error( InvalidHandle != bloom_, "Invalid handle" );
            throw_logic_error( byte_no < BloomSize, "Invalid Bloom offset" );

            // seek & write data
            {
                auto[ ok, pos ] = Os::seek_file( bloom_, HeaderOffsets::of_Bloom + byte_no );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_Bloom + byte_no, RetCode::IoError );
            }
            {
                auto[ ok, written ] = Os::write_file( bloom_, &byte, 1 );
                throw_storage_file_error( ok && written == 1, RetCode::IoError );
            }
        }


        /** Starts new transaction

        @retval Transaction - transaction object
        @throw storage_file_error
        */
        Transaction open_transaction()
        {
            using namespace std;

            throw_logic_error( RetCode::Ok == status_, "Invalid file" );
            return Transaction{ *this, move( writer_ ), move( unique_lock{ write_mutex_ } ) };
        }


        /** Provides input stream buffer for given chunk

        @param [in] chain - uid of start chunk of chain to be read
        @return istreambuf - input stream buffer
        @throw storage_file_error
        */
        template < typename CharT >
        istreambuf< CharT > get_chain_reader( ChunkUid chain )
        {
            using namespace std;

            throw_logic_error( RetCode::Ok == status_, "Invalid file" );

            // acquire reading handle
            unique_lock lock{ readers_mutex_ };

            readers_cv_.wait( lock, [&] { return !reader_stack_.empty(); } );

            auto reader = move( reader_stack_.top() );
            reader_stack_.pop();

            return istreambuf< CharT >{ *this, move( reader ), chain };
        }
    };
}


#include "transaction.h"
#include "streambufs.h"


#endif
