#ifndef __JB__TRANSACTION__H__
#define __JB__TRANSACTION__H__


#include <mutex>

#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


namespace jb
{
    /** Represents transaction to storage file

    @tparam Policies - global setting
    @tparam Pad - test stuff
    */
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl::StorageFile::Transaction
    {
        template < typename T > friend class TestStorageFile;

        //
        // needs access to private constructor
        //
        friend class StorageFile;

        //
        // needs access to private write()
        //
        template < typename CharT > friend class ostreambuf;

        //
        // data members
        //
        StorageFile & file_;
        std::unique_lock< std::mutex > write_lock_;
        streamer_t & writer_;
        uint64_t file_size_;
        ChunkUid free_space_;
        ChunkUid released_head_ = InvalidChunkUid, released_tile_ = InvalidChunkUid;
        ChunkUid first_written_chunk = InvalidChunkUid;
        ChunkUid last_written_chunk_ = InvalidChunkUid;
        ChunkUid overwritten_chunk_ = InvalidChunkUid;
        bool overwriting_used_ = false;
        bool overwriting_first_chunk_ = false;
        bool commited_ = false;


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


        /* Explicit private constructor

        @param [in] file - related file object
        @param [in] writer - write handle and associated i/o buffer
        @param [in] lock - write lock over the file
        @throw storage_file_error
        */
        explicit Transaction( StorageFile & file, streamer_t & writer, std::unique_lock< std::mutex > && lock )
            : file_( file )
            , writer_( writer )
            , write_lock_( move( lock ) )
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file object" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // read transactional data: file size...
            big_uint64_t file_size;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &file_size, sizeof( file_size ) );
                throw_storage_file_error( ok && read == sizeof( file_size ), RetCode::IoError );
            }
            file_size_ = file_size;

            // ... and free space
            big_uint64_t free_space;
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &free_space, sizeof( free_space ) );
                throw_storage_file_error( ok && read == sizeof( free_space ), RetCode::IoError );
            }
            free_space_ = free_space;

            // invalidate preserved chunk
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target, RetCode::IoError );
            }
            {
                big_uint64_t target = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( handle, &target, sizeof( target ) );
                throw_storage_file_error( ok && written == sizeof( target ), RetCode::IoError );
            }
        }


        /* Provides next available chunk uid

        @retval uint64_t - next available chunk
        @throw storage_file_error
        */
        [[nodiscard]]
        auto get_next_chunk()
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            ChunkUid available_chunk = InvalidChunkUid;

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
                {
                    auto[ ok, pos ] = Os::seek_file( handle, free_space_ + ChunkOffsets::of_NextFree );
                    throw_storage_file_error( ok && pos == free_space_ + ChunkOffsets::of_NextFree, RetCode::IoError );
                }
                {
                    auto[ ok, read ] = Os::read_file( handle, &next_free, sizeof( next_free ) );
                    throw_storage_file_error( ok && read == sizeof( next_free ), RetCode::IoError );
                }

                // move free space pointer to next free chunk
                free_space_ = next_free;
            }
            else
            {
                // get next available chunk at the end of file
                available_chunk = file_size_;

                // resize file
                auto[ ok, size ] = Os::resize_file( handle, file_size_ + sizeof( chunk_t ) );
                throw_storage_file_error( ok && size == file_size_ + sizeof( chunk_t ), RetCode::IoError );

                file_size_ = size;
            }

            return available_chunk;
        }


        /* Writes data coming from output stream

        @param [in] buffer - buffer to be written
        @param [in] buffer_size - number of bytes to be written
        @retval number of written bytes
        @throw storage_file_error
        */
        [[nodiscard]]
        size_t write( const void * buffer, size_t buffer_size )
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );
            throw_logic_error( buffer && buffer_size, "Invalid writing buffer" );
            throw_logic_error( !commited_, "Transaction is already finalized" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // get next chunk uid
            ChunkUid chunk_uid = get_next_chunk();

            // update last chunk in chain
            if ( last_written_chunk_ != InvalidChunkUid )
            {
                {
                    auto[ ok, pos ] = Os::seek_file( handle, last_written_chunk_ + ChunkOffsets::of_NextUsed );
                    throw_storage_file_error( ok && pos == last_written_chunk_ + ChunkOffsets::of_NextUsed, RetCode::IoError );
                }
                {
                    big_uint64_t next_used = chunk_uid;
                    auto[ ok, written ] = Os::write_file( handle, &next_used, sizeof( next_used ) );
                    throw_storage_file_error( ok && written == sizeof( next_used ), RetCode::IoError );
                }
            }

            // set the chunk as the last in chain
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk_uid + ChunkOffsets::of_NextUsed );
                throw_storage_file_error( ok && pos == chunk_uid + ChunkOffsets::of_NextUsed, RetCode::IoError );
            }
            {
                big_uint64_t next_used = InvalidChunkUid;
                auto[ ok, written ] = Os::write_file( handle, &next_used, sizeof( next_used ) );
                throw_storage_file_error( ok && written == sizeof( next_used ), RetCode::IoError );
            }

            // write the data size to chunk
            auto bytes_to_write = std::min( buffer_size, static_cast< size_t >( ChunkSize ) );
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk_uid + ChunkOffsets::of_UsedSize );
                throw_storage_file_error( ok && pos == chunk_uid + ChunkOffsets::of_UsedSize, RetCode::IoError );
            }
            {
                big_uint32_t bytes_no = static_cast< uint32_t >( bytes_to_write );
                auto[ ok, written ] = Os::write_file( handle, &bytes_no, sizeof( bytes_no ) );
                throw_storage_file_error( ok && written == sizeof( bytes_no ), RetCode::IoError );
            }

            // write the data to chunk
            size_t bytes_written = 0;
            {
                auto[ ok, pos ] = Os::seek_file( handle, chunk_uid + ChunkOffsets::of_Space );
                throw_storage_file_error( ok && pos == chunk_uid + ChunkOffsets::of_Space, RetCode::IoError );
            }
            {
                auto[ ok, written ] = Os::write_file( handle, buffer, bytes_to_write );
                bytes_written = static_cast< size_t >( written );
                throw_storage_file_error( ok && written == bytes_to_write, RetCode::IoError );
            }

            //
            // another Schrodinger chunk, it's allocated and released in the same time. The real state depends
            // on transaction completion
            //

            // remember first written chunk
            first_written_chunk = ( first_written_chunk == InvalidChunkUid ) ? chunk_uid : first_written_chunk;

            // remember this chunk as the last in chain
            last_written_chunk_ = chunk_uid;

            return bytes_written;
        }


    public:

        /** The class is not default constructible/copyable
        */
        Transaction() = delete;
        Transaction( const Transaction & ) = delete;
        Transaction & operator = ( const Transaction & ) = delete;


        /** But movable
        */
        Transaction( Transaction&& ) noexcept = default;


        /** Destructor

        Rolls back uncomited transaction and releases write lock over the file

        @throw nothing
        */
        ~Transaction()
        {
            if ( !commited_ ) file_.rollback();
        }


        /** Provides streaming buffer for overwriting of existing chain with preservation of start chunk uid

        @tparam CharT - type of character to be used by stream
        @param [in] uid - uid of chain to be overwritten
        @retval output stream buffer object
        @throw storage_file_error
        */
        template< typename CharT >
        ostreambuf< CharT > get_chain_overwriter( ChunkUid uid )
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );
            throw_logic_error( !commited_, "Transaction is already finalized" );
            throw_logic_error( !overwriting_used_, "Overwriting already used" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // initializing
            overwriting_used_ = true;
            overwriting_first_chunk_ = true;
            overwritten_chunk_ = uid;
            first_written_chunk = last_written_chunk_ = InvalidChunkUid;

            // write uid to be overwritten
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_PreservedChunk + PreservedChunkOffsets::of_Target, RetCode::IoError );
            }
            {
                big_uint64_t preserved_chunk = overwritten_chunk_;
                auto[ ok, written ] = Os::write_file( handle, &preserved_chunk, sizeof( preserved_chunk ) );
                throw_storage_file_error( ok && written == sizeof( preserved_chunk ), RetCode::IoError );
            }

            // mark 2nd and futher chunks of overwritten chain as released
            big_uint64_t second_chunk;
            {
                auto[ ok, pos ] = Os::seek_file( handle, overwritten_chunk_ + ChunkOffsets::of_NextUsed );
                throw_storage_file_error( ok && pos == overwritten_chunk_ + ChunkOffsets::of_NextUsed, RetCode::IoError );
            }
            {
                auto[ ok, read ] = Os::read_file( handle, &second_chunk, sizeof( second_chunk ) );
                throw_storage_file_error( ok && read == sizeof( second_chunk ), RetCode::IoError );
            }

            if ( InvalidChunkUid != second_chunk ) erase_chain( second_chunk );

            return ostreambuf< CharT >( *this, writer_ );
        }


        /** Provides streaming buffer for writting a chain

        @tparam CharT - type of character to be used by stream
        @retval output stream buffer object
        @throw storage_file_error
        */
        template< typename CharT >
        auto get_chain_writer()
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );
            throw_logic_error( !commited_, "Transaction is already finalized" );

            first_written_chunk = last_written_chunk_ = InvalidChunkUid;

            // provide streambuf object
            return ostreambuf< CharT >( *this, writer_ );
        }


        /** Provides uid of the first chunk in written chain

        @retval ChunkUid - uid of the first written chunk
        @throw storgae_file_error
        */
        [[nodiscard]]
        ChunkUid get_first_written_chunk()
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );

            return first_written_chunk;
        }


        /** Marks a chain started from given chunk as released

        @param [in] chunk - staring chunk
        @throw storgae_file_error
        */
        auto erase_chain( ChunkUid chunk )
        {
            using namespace std;

            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );
            throw_logic_error( !commited_, "Transaction is already finalized" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // check that the chain is valid
            throw_logic_error( HeaderOffsets::of_Root < chunk && chunk < file_size_, "Invalid chain" );

            // initialize pointers to released space end
            released_tile_ = ( released_tile_ == InvalidChunkUid ) ? chunk : released_tile_;

            // till end of chain
            while ( chunk != InvalidChunkUid )
            {
                // get next used for current chunk
                big_uint64_t next_used;
                {
                    auto[ ok, pos ] = Os::seek_file( handle, chunk + ChunkOffsets::of_NextUsed );
                    throw_storage_file_error( ok && pos == chunk + ChunkOffsets::of_NextUsed, RetCode::IoError );
                }
                {
                    auto[ ok, read ] = Os::read_file( handle, &next_used, sizeof( next_used ) );
                    throw_storage_file_error( ok && read == sizeof( next_used ), RetCode::IoError );
                }

                // set next free to released head
                big_uint64_t next_free = released_head_;
                {
                    auto[ ok, pos ] = Os::seek_file( handle, chunk + ChunkOffsets::of_NextFree );
                    throw_storage_file_error( ok && pos == chunk + ChunkOffsets::of_NextFree, RetCode::IoError );
                }
                {
                    auto[ ok, written ] = Os::write_file( handle, &next_free, sizeof( next_free ) );
                    throw_storage_file_error( ok && written == sizeof( next_free ), RetCode::IoError );
                }

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
        }


        /** Commit transaction

        @throw storgae_file_error
        */
        auto commit() 
        {
            throw_logic_error( RetCode::Ok == file_.status(), "Invalid file" );
            throw_logic_error( !commited_, "Transaction is already finalized" );

            Handle & handle = writer_.first;
            throw_logic_error( InvalidHandle != handle, "Invalid file handle" );

            // if there is released space - glue released chunks with remaining free space
            if ( released_head_ != InvalidChunkUid )
            {
                assert( released_tile_ != InvalidChunkUid );

                {
                    auto[ ok, pos ] = Os::seek_file( handle, released_tile_ + ChunkOffsets::of_NextFree );
                    throw_storage_file_error( ok && pos == released_tile_ + ChunkOffsets::of_NextFree, RetCode::IoError );
                }
                {
                    big_uint64_t next_free = InvalidChunkUid;
                    auto[ ok, written ] = Os::write_file( handle, &next_free, sizeof( next_free ) );
                    throw_storage_file_error( ok && written == sizeof( next_free ), RetCode::IoError );
                }
                free_space_ = released_head_;
            }

            // write transaction
            header_t::transactional_data_t transaction{ file_size_, free_space_ };

            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_Transaction );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_Transaction, RetCode::IoError );
            }
            {
                auto[ ok, written ] = Os::write_file( handle, &transaction, sizeof( transaction ) );
                throw_storage_file_error( ok && written == sizeof( transaction ), RetCode::IoError );
            }

            // and finalize transaction with CRC
            {
                auto[ ok, pos ] = Os::seek_file( handle, HeaderOffsets::of_TransactionCrc );
                throw_storage_file_error( ok && pos == HeaderOffsets::of_TransactionCrc, RetCode::IoError );
            }
            {
                boost::endian::big_uint64_t crc = variadic_hash( ( uint64_t )transaction.file_size_, ( uint64_t )transaction.free_space_ );
                auto[ ok, written ] = Os::write_file( handle, &crc, sizeof( crc ) );
                throw_storage_file_error( ok && written == sizeof( crc ), RetCode::IoError );
            }

            //
            // now we have valid transaction ready to apply
            //

            // force file to apply commit
            file_.commit();

            // mark transaction as commited
            commited_ = true;
        }
    };
}

#endif


