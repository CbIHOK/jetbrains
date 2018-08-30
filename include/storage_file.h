#ifndef __JB__STORAGE_FILE__H__
#define __JB__STORAGE_FILE__H__


#include <filesystem>
#include <string>
#include <array>
#include <execution>
#include <stack>
#include <mutex>
#include <limits>
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
        //using big_uint64_t = boost::endian::big_uint64_at;
        using big_int64_t = boost::endian::big_int64_at;

        inline static const Handle InvalidHandle = Os::InvalidHandle;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;
        static constexpr auto ReaderNumber = Policies::PhysicalVolumePolicy::ReaderNumber;
        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static constexpr auto ChunkSize = Policies::PhysicalVolumePolicy::ChunkSize;


    public:

        // declares chunk uid
        using ChunkUid = int64_t;
        static constexpr ChunkUid InvalidChunkUid = std::numeric_limits< ChunkUid >::max();

        //
        // few forwarding declarations
        //
        class Transaction;
        template < typename CharT > class ostreambuf;
        template < typename CharT > class istreambuf;


    private:

        // status
        RetCode status_ = RetCode::Ok;
        bool newly_created_ = false;

        // file locking
        std::string file_lock_name_;
        std::shared_ptr< boost::interprocess::named_mutex > file_lock_;

        // writing objects
        mutable std::mutex transaction_mutex_;
        Handle writer_ = InvalidHandle;

        // bloom writer
        Handle bloom_ = InvalidHandle;

        // readers
        std::mutex readers_mutex_;
        std::condition_variable readers_cv_;
        boost::container::static_vector< Handle, ReaderNumber > readers_;
        using reader_stack_t = std::stack< Handle, boost::container::static_vector< Handle, ReaderNumber > >;
        reader_stack_t reader_stack_;


        /* Generates compatibility stamp

        @retval unique stamp of software settings
        @throw nothing
        */
        static uint64_t generate_compatibility_stamp() noexcept
        {
            using namespace std;
            auto hash = variadic_hash( type_index( typeid( Key ) ), type_index( typeid( ValueT ) ), BloomSize, MaxTreeDepth, BTreeMinPower, ChunkSize );
            return hash;
        }

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
            big_int64_t next_used_;                   //< next used chunk (takes sense for allocated chunks)
            big_int64_t next_free_;                   //< next free chunk (takes sense for released chunks)
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
            boost::endian::big_int64_t compatibility_stamp;        //< software compatibility stamp

            uint8_t bloom_[ BloomSize ];              //< bloom filter data

            struct transactional_data_t
            {
                big_int64_t file_size_;             //< current file size
                big_int64_t free_space_;            //< pointer to first free chunk (garbage collector)
            };

            transactional_data_t transactional_data_; //< original copy
            transactional_data_t transaction_;        //< transaction copy
            boost::endian::big_uint64_t transaction_crc_;           //< transaction CRC (identifies valid transaction)

            struct preserved_chunk_t
            {
                big_int64_t target_;                  //< target chunk uid
                chunk_t chunk_;                       //< preserved chunk
            };
            preserved_chunk_t overwritten__chunk_;       //< let us make one writing per transaction with preservation of original chunk uid
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

        // declares uid of the Root chunk
        static constexpr ChunkUid RootChunkUid = static_cast< ChunkUid >( HeaderOffsets::of_Root );


    private:


        /* Check software to file compatibility

        @throws nothing
        @warning  not thread safe
        */
        [[nodiscard]]
        auto check_compatibility() noexcept
        {
            //conditional executor
            RetCode status = RetCode::Ok;
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status ) status = f(); };

            boost::endian::big_uint64_t stamp;

            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_CompatibilityStamp );
                return ( ok && pos == HeaderOffsets::of_CompatibilityStamp ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( writer_, &stamp, sizeof( stamp ) );
                return ( ok && read == sizeof( stamp ) ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                return ( stamp != generate_compatibility_stamp() ) ? RetCode::IncompatibleFile : RetCode::Ok;
            } );

            return status;
        }


        /* Initialize newly create file

        @retvar RetCode - operation status
        @throw nothing
        @warning not thread safe
        */
        [[nodiscard]]
        auto deploy() noexcept
        {
            //conditional executor
            RetCode status = RetCode::Ok;

            auto ce = [&] ( const auto & f ) noexcept {
                if ( RetCode::Ok == status ) status = f();
                assert( RetCode::Ok == status );
            };

            // reserve space for header
            ce( [&] {
                auto[ ok, size ] = Os::resize_file( writer_, sizeof( header_t ) );
                return ok && size == sizeof( header_t ) ? RetCode::Ok : RetCode::IoError;
            } );

            // write compatibility stamp
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_CompatibilityStamp );
                return ( ok && pos == HeaderOffsets::of_CompatibilityStamp ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                boost::endian::big_uint64_t stamp = generate_compatibility_stamp();
                auto[ ok, written ] = Os::write_file( writer_, &stamp, sizeof( stamp ) );
                return ( ok && written == sizeof( stamp ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // write file size
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_int64_t file_size = HeaderOffsets::of_Root;
                auto[ ok, written ] = Os::write_file( writer_, &file_size, sizeof( file_size ) );
                return ( ok && written == sizeof( file_size ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // invalidate free space ptr
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace );
                return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_int64_t free_space = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( writer_, &free_space, sizeof( free_space ) );
                return ( ok && written == sizeof( free_space ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // invalidate transaction
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionCrc );
                return ( ok && pos == HeaderOffsets::of_TransactionCrc ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                boost::endian::big_uint64_t invalid_crc = variadic_hash( HeaderOffsets::of_Root, InvalidChunkUid ) + 1;
                auto[ ok, written ] = Os::write_file( writer_, &invalid_crc, sizeof( invalid_crc ) );
                return ( ok && written == sizeof( invalid_crc ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            //
            ce( [&] {
                try
                {
                    //auto t = open_transaction();
                    //{
                    //    auto osbuf = t.get_chain_writer< int64_t >();
                    //    std::ostream os( &osbuf );
                    //    boost::archive::binary_oarchive ar( os );
                    //    BTree root;
                    //    ar & root;
                    //    //os.flush();
                    //}
                    //return t.commit();
                    return RetCode::Ok;
                }
                catch ( ... )
                {
                    return RetCode::UnknownError;
                }
            } );

            return status;
        }


        /* Applies transaction

        @retval operation status
        @throw nothing
        */
        [[nodiscard]]
        auto commit() const noexcept
        {
            RetCode status = RetCode::Ok;

            auto ce = [&] ( const auto & f ) noexcept {
                if ( RetCode::Ok == status ) status = f();
                assert( RetCode::Ok == status || RetCode::UnknownError == status );
            };

            // read transaction data
            header_t::transactional_data_t transaction;

            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_Transaction );
                return ( ok && pos == HeaderOffsets::of_Transaction ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( writer_, &transaction, sizeof( transaction ) );
                return ( ok && read == sizeof( transaction ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // read CRC
            boost::endian::big_uint64_t transaction_crc;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionCrc );
                return ( ok && pos == HeaderOffsets::of_TransactionCrc ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( writer_, &transaction_crc, sizeof( transaction_crc ) );
                return ( ok && read == sizeof( transaction_crc ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // validate transaction
            bool valid_transaction = false;
            ce( [&] {
                uint64_t file_size = transaction.file_size_, free_space = transaction.free_space_;
                valid_transaction = transaction_crc == variadic_hash( file_size, free_space );
                return RetCode::Ok;
            } );

            if ( valid_transaction )
            {
                // read preserved chunk target
                big_int64_t preserved_target;
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                    return ( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, read ] = Os::read_file( writer_, &preserved_target, sizeof( preserved_target ) );
                    return ( ok && read == sizeof( preserved_target ) ) ? RetCode::Ok : RetCode::IoError;
                } );

                // if there is preserved chunk
                if ( preserved_target != InvalidChunkUid )
                {
                    // read preserved chunk
                    chunk_t preserved_chunk;
                    ce( [&] {
                        auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Chunk );
                        return ( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Chunk ) ? RetCode::Ok : RetCode::IoError;
                    } );
                    ce( [&] {
                        auto[ ok, read ] = Os::read_file( writer_, &preserved_chunk, sizeof( preserved_chunk ) );
                        return ( ok && read == sizeof( preserved_chunk ) ) ? RetCode::Ok : RetCode::IoError;
                    } );

                    // copy preserved chunk to target
                    ce( [&] {
                        auto[ ok, pos ] = Os::seek_file( writer_, preserved_target );
                        return ( ok && pos == preserved_target ) ? RetCode::Ok : RetCode::IoError;
                    } );
                    ce( [&] {
                        auto[ ok, written ] = Os::write_file( writer_, &preserved_chunk, sizeof( preserved_chunk ) );
                        return ( ok && written == sizeof( preserved_chunk ) ) ? RetCode::Ok : RetCode::IoError;
                    } );
                }

                // apply transaction
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionalData );
                    return ( ok && pos == HeaderOffsets::of_TransactionalData ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, written ] = Os::write_file( writer_, &transaction, sizeof( transaction ) );
                    return ( ok && written == sizeof( transaction ) ) ? RetCode::Ok : RetCode::IoError;
                } );

                // invalidate transaction
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionCrc );
                    return ( ok && pos == HeaderOffsets::of_TransactionCrc ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    uint64_t file_size = transaction.file_size_, free_space = transaction.free_space_;
                    boost::endian::big_uint64_t invalid_crc = variadic_hash( file_size, free_space ) + 1;
                    auto[ ok, written ] = Os::write_file( writer_, &invalid_crc, sizeof( invalid_crc ) );
                    return ( ok && written == sizeof( invalid_crc ) ) ? RetCode::Ok : RetCode::IoError;
                } );
            }
            else
            {
                // just restore file size
                big_int64_t file_size;
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                    return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, read ] = Os::read_file( writer_, &file_size, sizeof( file_size ) );
                    return ( ok && read == sizeof( file_size ) ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, size ] = Os::resize_file( writer_, file_size );
                    return ( ok && size == file_size ) ? RetCode::Ok : RetCode::IoError;
                } );
            }

            // done
            return status;
        }


        /* Rollback transaction

        @throw noexcept
        */
        auto rollback() noexcept
        {
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // just restore file size
            big_int64_t file_size;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( writer_, &file_size, sizeof( file_size ) );
                return ( ok && read == sizeof( file_size ) ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, size ] = Os::resize_file( writer_, file_size );
                return ( ok && size == file_size ) ? RetCode::Ok : RetCode::IoError;
            } );
        }


    public:

        /** The class is not default creatable/copyable/movable
        */
        StorageFile() = delete;
        StorageFile( StorageFile && ) = delete;


        /** Destructor, releases allocated resources
        */
        ~StorageFile()
        {
            using namespace std;

            // release writer
            if ( writer_ != InvalidHandle ) Os::close_file( writer_ );

            // release bloom data writer
            if ( bloom_ != InvalidHandle ) Os::close_file( bloom_ );

            // release readers
            assert( readers_.size() == ReaderNumber );
            for_each( begin( readers_ ), end( readers_ ), [] ( auto h ) {
                if ( h != InvalidHandle ) Os::close_file( h );
            } );

            // unlock file name
            if ( file_lock_ )
            {
                file_lock_.reset();
                boost::interprocess::named_mutex::remove( file_lock_name_.c_str() );
            }
        }


        /** Constructs an instance

        @param [in] path - path to physical file
        @param [in] suppress_lock - do not lock the file (test mode)
        @throw nothing
        */
        explicit StorageFile( const std::filesystem::path & path, bool suppress_lock = false ) try
            : file_lock_name_{ "jb_lock_" + std::to_string( std::filesystem::hash_value( path ) ) }
            , readers_( ReaderNumber, InvalidHandle )
        {
            using namespace std;

            if ( !suppress_lock )
            {
                //
                // Unfortunately MS does not care about standards as usual and HANDLED exceptions easily leaves
                // try-catch constructors by an rethrow. That is why I have to use such workaround
                //
                try
                {
                    // lock file
                    file_lock_ = std::make_shared< boost::interprocess::named_mutex >( boost::interprocess::create_only_t{}, file_lock_name_.c_str() );
                }
                catch ( const boost::interprocess::interprocess_exception & )
                {
                    status_ = RetCode::AlreadyOpened;
                }
            }

            // conditional executor
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // open writter
            ce( [&] {
                auto[ opened, tried_create, handle ] = Os::open_file( path );

                newly_created_ = tried_create;
                writer_ = handle;

                return opened ? RetCode::Ok : RetCode::UnableToOpen;
            } );

            // deploy new file
            ce( [&] {
                return newly_created_ ? deploy() : RetCode::Ok;
            } );

            // check compatibility for existing file
            ce( [&] {
                return newly_created_ ? RetCode::Ok : check_compatibility();
            } );

            // apply valid transaction if exists
            ce( [&] {
                if ( !newly_created_ )
                {
                    auto ret = commit();
                    return ( RetCode::Ok == ret || RetCode::UnknownError == ret ) ? RetCode::Ok : RetCode::UnknownError;
                }
                else
                {
                    return RetCode::Ok;
                }
            } );

            // open Bloom writer
            ce( [&] {
                auto[ opened, tried_create, handle ] = Os::open_file( path );
                return ( bloom_ = handle ) != InvalidHandle ? RetCode::Ok : RetCode::UnableToOpen;
            } );

            // open readers
            for ( auto & reader : readers_ )
            {
                ce( [&] {
                    auto[ opened, tried_create, handle ] = Os::open_file( path );
                    return ( reader = handle ) != InvalidHandle ? RetCode::Ok : RetCode::UnableToOpen;
                } );
            }

            // initialize stack of readers
            reader_stack_ = move( reader_stack_t{ readers_ } );
        }
        catch ( const std::bad_alloc & )
        {
            status_ = RetCode::InsufficientMemory;
        }
        catch ( ... )
        {
            status_ = RetCode::UnknownError;
        }


        /** Provides creation status

        @throw nothing
        */
        [[nodiscard]]
        auto status() const noexcept { return status_; }


        [[nodiscard]]
        auto newly_created() const noexcept { return newly_created_; }


        /** Reads data for Bloom filter

        @param [out] bloom_buffer - target for Bloom data
        @return RetCode - status of operation
        @throw nothing
        @wraning not thread safe
        */
        [[nodiscard]]
        RetCode read_bloom( uint8_t * bloom_buffer ) const noexcept
        {
            if ( !newly_created_ )
            {
                RetCode status = RetCode::Ok;
                auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status ) status = f(); };

                // seek & read data
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( bloom_, HeaderOffsets::of_Bloom );
                    return ok && pos == HeaderOffsets::of_Bloom ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, read ] = Os::read_file( bloom_, bloom_buffer, BloomSize );
                    return ok && read == BloomSize ? RetCode::Ok : RetCode::IoError;
                } );

                return status;
            }
            else
            {
                assert( reinterpret_cast< size_t >( bloom_buffer ) % sizeof( uint64_t ) == 0 && BloomSize % sizeof( uint64_t ) == 0 );

                // STOSB -> STOSQ
                const auto start = reinterpret_cast< uint64_t* >( bloom_buffer );
                const auto end = start + BloomSize / sizeof( uint64_t );
                std::fill( start, end, 0 );

                return RetCode::Ok;
            }
        }


        /** Write changes from Bloom filter

        @param [in] byte_no - ordinal number of byte in the array
        @param [in] byte - value to be written
        @retval - operation status
        @thrown nothing
        @note the function is not thread safe, but it's guaranied by the caller
        */
        [[nodiscard]]
        RetCode add_bloom_digest( size_t byte_no, uint8_t byte ) const noexcept
        {
            using namespace std;

            assert( byte_no < BloomSize );

            RetCode status = RetCode::Ok;
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status ) status = f(); };

            // seek & write data
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( bloom_, HeaderOffsets::of_Bloom + byte_no );
                return ok && pos == HeaderOffsets::of_Bloom + byte_no ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, written ] = Os::write_file( bloom_, &byte, 1 );
                return ok && written == 1 ? RetCode::Ok : RetCode::IoError;
            } );

            return status;
        }


        [[nodiscard]]
        std::tuple< RetCode, size_t, ChunkUid > read_chunk( Handle reader, ChunkUid chunk, void * buffer, size_t buffer_size ) const noexcept
        {
            using namespace std;

            assert( InvalidHandle != reader );
            assert( InvalidChunkUid != chunk );
            assert( buffer && buffer_size );

            RetCode status = RetCode::Ok;

            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status ) status = f(); };

            // validate chunk uid
            ce( [&] {
                return HeaderOffsets::of_Root <= chunk ? RetCode::Ok : RetCode::UnknownError;
            } );

            // get next used
            big_int64_t next_used;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( reader, chunk + ChunkOffsets::of_NextUsed );
                return ( ok && pos == chunk + ChunkOffsets::of_NextUsed ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( reader, &next_used, sizeof( next_used ) );
                return ( ok && read == sizeof( next_used ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // get size of utilized space in chunk
            big_uint32_t used_size;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( reader, chunk + ChunkOffsets::of_UsedSize );
                return ( ok && pos == chunk + ChunkOffsets::of_UsedSize ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( reader, &used_size, sizeof( used_size ) );
                return ( ok && read == sizeof( used_size ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // read size
            auto size_to_read = min( buffer_size, static_cast< size_t >( used_size ) );

            // read data
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( reader, chunk + ChunkOffsets::of_Space );
                return ( ok && pos == chunk + ChunkOffsets::of_Space ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( reader, buffer, size_to_read );
                return ( ok && read == size_to_read ) ? RetCode::Ok : RetCode::IoError;
            } );

            return  RetCode::Ok == status ? tuple{ RetCode::Ok, size_to_read, next_used } : tuple{ status, 0, InvalidChunkUid };
        }


        /** Starts new transaction
        */
        Transaction open_transaction() const noexcept
        {
            using namespace std;
            return Transaction{ const_cast< StorageFile* >( this ), writer_, move( unique_lock{ transaction_mutex_ } ) };
        }


        /** Provides input stream buffer for given chunk

        @param [in] chain - uid of start chunk of chain to be read
        @return stream buffer object
        @throw nothing
        @note in theory the body may fire an exception, but in this case the better to die on noexcept
              and analyze the crash than investigate a deadlock
        */
        template < typename CharT >
        istreambuf< CharT > get_chain_reader( ChunkUid chain ) noexcept
        {
            using namespace std;

            Handle reader = InvalidHandle;

            // acquire reading handle
            {
                unique_lock lock( readers_mutex_ );
                readers_cv_.wait( lock, [&] { return !reader_stack_.empty(); } );
                
                reader = reader_stack_.top();
                reader_stack_.pop();
            }

            return istreambuf< CharT >{ this, reader, chain };
        }
    };


    /** Represents transaction to storage file

    @tparam Policies - global setting
    @tparam Pad - test stuff
    */
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl::StorageFile::Transaction
    {
        template < typename T > friend class TestStorageFile;
        friend class StorageFile;
        template < typename CharT > friend class ostreambuf;

        RetCode status_ = RetCode::UnknownError;
        StorageFile * file_ = nullptr;
        std::unique_lock< std::mutex > lock_;
        Handle handle_ = InvalidHandle;
        int64_t file_size_;
        ChunkUid free_space_;
        ChunkUid released_head_ = InvalidChunkUid, released_tile_ = InvalidChunkUid;
        ChunkUid first_written_chunk = InvalidChunkUid;
        ChunkUid last_written_chunk_ = InvalidChunkUid;
        ChunkUid overwritten__chunk_ = InvalidChunkUid;
        bool overwriting_used_ = false;
        bool overwriting_first_chunk_ = false;
        bool commited_ = false;

        /* Constructor

        @param [in] file - related file object
        @param [in] lock - write lock
        @throw nothing
        */
        explicit Transaction( StorageFile * file, Handle handle, std::unique_lock< std::mutex > && lock ) noexcept
            : file_{ file }
            , handle_{ handle }
            , lock_{ move( lock ) }
            , status_{ RetCode::Ok }
        {
            assert( file_ );
            assert( handle_ != InvalidHandle );

            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // read transactional data: file size...
            big_int64_t file_size;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( handle_, &file_size, sizeof( file_size ) );
                return ( ok && read == sizeof( file_size ) ? RetCode::Ok : RetCode::IoError );
            } );
            file_size_ = file_size;

            // ... and free space
            big_int64_t free_space;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace );
                return ( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( handle_, &free_space, sizeof( free_space ) );
                return ( ok && read == sizeof( free_space ) ? RetCode::Ok : RetCode::IoError );
            } );
            free_space_ = free_space;

            // invalidate preserved chunk
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                return ( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_int64_t target = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( handle_, &target, sizeof( target ) );
                return ( ok && written == sizeof( target ) ? RetCode::Ok : RetCode::IoError );
            } );
        }

        
        /* Provides next available chunk uid

        @retval RetCode - operation status
        @retval uint64_t - next available chunk
        @throw nothing
        */
        [[nodiscard]]
        std::tuple< RetCode, ChunkUid > get_next_chunk() noexcept
        {
            ChunkUid available_chunk = InvalidChunkUid;

            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // if preserved writting: return reserved chunk offset
            if ( overwriting_first_chunk_ )
            {
                available_chunk = HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Chunk;
                overwriting_first_chunk_ = false;
            }
            // if there is free space
            else if ( free_space_ != InvalidChunkUid )
            {
                // retrieve next available chunk from free space
                available_chunk = free_space_;

                // read next free chunk
                boost::endian::big_uint64_t next_free;
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( handle_, free_space_ + ChunkOffsets::of_NextFree );
                    return ( ok && pos == free_space_ + ChunkOffsets::of_NextFree ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, read ] = Os::read_file( handle_, &next_free, sizeof( next_free ) );
                    return ( ok && read == sizeof( next_free ) ? RetCode::Ok : RetCode::IoError );
                } );

                // move free space pointer to next free chunk
                free_space_ = next_free;
            }
            else
            {
                // get next available chunk at the end of file
                available_chunk = file_size_;

                // resize file
                ce( [&] {
                    auto[ ok, size ] = Os::resize_file( handle_, file_size_ + sizeof( chunk_t ) );
                    if ( ok && size == file_size_ + sizeof( chunk_t ) )
                    {
                        file_size_ = size;
                        return RetCode::Ok;
                    }
                    else
                    {
                        return RetCode::IoError;
                    }
                } );
            }

            return { status_, available_chunk };
        }


        /* Writes data coming through output stream

        @param [in] data - data to be written
        @param [in] sz - data size
        @retval operation status
        @throw nothing
        */
        [[nodiscard]]
        std::tuple< RetCode, size_t > write( const void * buffer, size_t buffer_size ) noexcept
        {
            using namespace std;

            assert( buffer && buffer_size );

            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // get next chunk uid
            ChunkUid chunk_uid;
            ce( [&] {
                auto [ rc, next_chunk ] = get_next_chunk();
                chunk_uid = next_chunk;
                return rc;
            } );

            // update last chunk in chain
            if ( last_written_chunk_ != InvalidChunkUid )
            {
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( handle_, last_written_chunk_ + ChunkOffsets::of_NextUsed );
                    return ( ok && pos == last_written_chunk_ + ChunkOffsets::of_NextUsed ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    big_int64_t next_used = chunk_uid;
                    auto[ ok, written ] = Os::write_file( handle_, &next_used, sizeof( next_used ) );
                    return ( ok && written == sizeof( next_used ) ) ? RetCode::Ok : RetCode::IoError;
                } );
            }

            // set the chunk as the last in chain
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, chunk_uid + ChunkOffsets::of_NextUsed );
                return ( ok && pos == chunk_uid + ChunkOffsets::of_NextUsed ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_int64_t next_used = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( handle_, &next_used, sizeof( next_used ) );
                return ( ok && written == sizeof( next_used ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // write the data size to chunk
            auto bytes_to_write = min( buffer_size, static_cast< size_t >( ChunkSize ) );
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, chunk_uid + ChunkOffsets::of_UsedSize );
                return ( ok && pos == chunk_uid + ChunkOffsets::of_UsedSize ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_uint32_t bytes_no = static_cast< uint32_t >( bytes_to_write );
                auto[ ok, written ] = Os::write_file( handle_, &bytes_no, sizeof( bytes_no ) );
                return ( ok && written == sizeof( bytes_no ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // write the data to chunk
            size_t bytes_written = 0;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, chunk_uid + ChunkOffsets::of_Space );
                return ( ok && pos == chunk_uid + ChunkOffsets::of_Space ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, written ] = Os::write_file( handle_, buffer, bytes_to_write );
                bytes_written = written;
                return ( ok && written == bytes_to_write ) ? RetCode::Ok : RetCode::IoError;
            } );

            //
            // another Schrodinger chunk, it's allocated and released in the same time. The real state depends
            // on transaction completion
            //

            // remember first written chunk
            first_written_chunk = ( first_written_chunk == InvalidChunkUid ) ? chunk_uid : first_written_chunk;

            // remember this chunk as the last in chain
            last_written_chunk_ = chunk_uid;

            return { status_, bytes_written };
        }


    public:

        /** Default constructor, creates dummy transaction
        */
        Transaction() noexcept = default;


        ~Transaction()
        {
            if ( !commited_ ) file_->rollback();
        }


        /** The class is not copyable
        */
        Transaction( const Transaction & ) = delete;
        Transaction & operator = ( const Transaction & ) = delete;


        /** But movable
        */
        Transaction( Transaction&& ) noexcept = default;


        /** Provides transaction status

        @retval status
        @throw nothing
        */
        [[nodiscard]]
        RetCode status() const noexcept { return status_; }


        /** Provides streaming buffer for overwriting of existing chain with preservation of start chunk uid

        @param [in] uid - uid of chain to be overwritten
        @retval output stream buffer object
        @throw nothing
        */
        template< typename CharT >
        std::tuple< RetCode, ostreambuf< CharT> > get_chain_overwriter( ChunkUid uid ) noexcept
        {
            assert( uid != InvalidChunkUid );

            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // overwriting allowed only for the chunks created before this transaction
            ce( [&] {
                return ( HeaderOffsets::of_Root <= uid && uid < file_size_ ) ? RetCode::Ok : RetCode::UnknownError;
            } );

            // check that overwriting has not been used during this transaction
            ce( [&] {
                return !overwriting_used_ ? RetCode::Ok : RetCode::UnknownError;
            } );

            // initializing
            ce( [&] {
                overwriting_used_ = true;
                overwriting_first_chunk_ = true;
                overwritten__chunk_ = uid;
                first_written_chunk = last_written_chunk_ = InvalidChunkUid;
                return RetCode::Ok;
            } );

            // write uid to be overwritten
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                return ( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                big_int64_t preserved_chunk = overwritten__chunk_;
                auto[ ok, written ] = Os::write_file( handle_, &preserved_chunk, sizeof( preserved_chunk ) );
                return ( ok && written == sizeof( preserved_chunk ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // mark 2nd and futher chunks of overwritten chain as released
            big_int64_t second_chunk;
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, overwritten__chunk_ + ChunkOffsets::of_NextUsed );
                return ( ok && pos == overwritten__chunk_ + ChunkOffsets::of_NextUsed ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, read ] = Os::read_file( handle_, &second_chunk, sizeof( second_chunk ) );
                return ( ok && read == sizeof( second_chunk ) ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                return second_chunk != InvalidChunkUid ? erase_chain( second_chunk ) : RetCode::Ok;
            } );

            return { status_, ostreambuf< CharT >{ const_cast< Transaction* >( this ) } };
        }


        /** Provides streaming buffer for writting a chain

        @retval output stream buffer object
        @throw nothing
        */
        template< typename CharT >
        ostreambuf< CharT > get_chain_writer() noexcept
        {
            first_written_chunk = last_written_chunk_ = InvalidChunkUid;

            // provide streambuf object
            return ostreambuf< CharT >( const_cast< Transaction* >( this ) );
        }


        /** Provides uid of the first chunk in written chain

        @retval ChunkUid - uid of the first chunk
        @throw nothing
        */
        [[nodiscard]]
        ChunkUid get_first_written_chunk() const noexcept { return first_written_chunk; }


        /** Marks a chain started from given chunk as released

        @param [in] chunk - staring chunk
        @retval operation status
        @throw nothing
        */
        [[nodiscard]]
        RetCode erase_chain( ChunkUid chunk ) noexcept
        {
            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // check that given chunk is valid
            //if ( chunk < HeaderOffsets::of_Root || file_size_ < chunk )
            //{
            //    return RetCode::UnknownError;
            //}

            // initialize pointers to released space end
            ce( [&] {
                released_tile_ = ( released_tile_ == InvalidChunkUid ) ? chunk : released_tile_;
                return RetCode::Ok;
            } );

            // till end of chain
            while ( chunk != InvalidChunkUid )
            {
                // get next used for current chunk
                big_int64_t next_used;
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( handle_, chunk + ChunkOffsets::of_NextUsed );
                    return ( ok && pos == chunk + ChunkOffsets::of_NextUsed ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, read ] = Os::read_file( handle_, &next_used, sizeof( next_used ) );
                    return ( ok && read == sizeof( next_used ) ) ? RetCode::Ok : RetCode::IoError;
                } );

                // set next free to released head
                big_int64_t next_free = released_head_;
                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( handle_, chunk + ChunkOffsets::of_NextFree );
                    return ( ok && pos == chunk + ChunkOffsets::of_NextFree ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    auto[ ok, written ] = Os::write_file( handle_, &next_free, sizeof( next_free ) );
                    return ( ok && written == sizeof( next_free ) ) ? RetCode::Ok : RetCode::IoError;
                } );

                //
                // set released chain head to the chunk
                //
                released_head_ = chunk;

                //
                // thus we've got Schrodinger chunk, it's allocated and released in the same time. The real state depends
                // on transaction completion
                //

                // go to next used chunk
                chunk = next_used;
            }

            return status_;
        }

        /** Commit transaction

        @retval - operation status
        @throw nothing
        */
        [[nodiscard]]
        RetCode commit() noexcept
        {
            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( RetCode::Ok == status_ ) status_ = f(); };

            // if there is released space - glue released chunks with remaining free space
            if ( released_head_ != InvalidChunkUid )
            {
                assert( released_tile_ != InvalidChunkUid );

                ce( [&] {
                    auto[ ok, pos ] = Os::seek_file( handle_, released_tile_ + ChunkOffsets::of_NextFree );
                    return ( ok && pos == released_tile_ + ChunkOffsets::of_NextFree ) ? RetCode::Ok : RetCode::IoError;
                } );
                ce( [&] {
                    big_int64_t next_free = free_space_;
                    auto[ ok, written ] = Os::write_file( handle_, &next_free, sizeof( next_free ) );
                    return ( ok && written == sizeof( next_free ) ) ? RetCode::Ok : RetCode::IoError;
                } );
                free_space_ = released_head_;
            }

            // write transaction
            header_t::transactional_data_t transaction{ file_size_, free_space_ };

            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_Transaction );
                return ( ok && pos == HeaderOffsets::of_Transaction ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                auto[ ok, written ] = Os::write_file( handle_, &transaction, sizeof( transaction ) );
                return ( ok && written == sizeof( transaction ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            // and finalize transaction with CRC
            ce( [&] {
                auto[ ok, pos ] = Os::seek_file( handle_, HeaderOffsets::of_TransactionCrc );
                return ( ok && pos == HeaderOffsets::of_TransactionCrc ) ? RetCode::Ok : RetCode::IoError;
            } );
            ce( [&] {
                boost::endian::big_uint64_t crc = variadic_hash( ( uint64_t )transaction.file_size_, ( uint64_t )transaction.free_space_ );
                auto[ ok, written ] = Os::write_file( handle_, &crc, sizeof( crc ) );
                return ( ok && written == sizeof( crc ) ) ? RetCode::Ok : RetCode::IoError;
            } );

            //
            // now we have valid transaction
            //

            // force file to apply commit
            ce( [&] {
                return file_->commit();
            } );

            // mark transaction as commited
            commited_ = true;

            return status_;
        }


    };
}


#include "streambufs.h"


#endif
