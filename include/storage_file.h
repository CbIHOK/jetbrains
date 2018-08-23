#ifndef __JB__STORAGE_FILE__H__
#define __JB__STORAGE_FILE__H__


#include <filesystem>
#include <string>
#include <array>
#include <execution>
#include <stack>
#include <mutex>
#include <limits>

#include <boost/container/static_vector.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/container_hash/hash.hpp>

#ifndef BOOST_ENDIAN_DEPRECATED_NAMES
#define BOOST_ENDIAN_DEPRECATED_NAMES
#include <boost/endian/endian.hpp>
#undef BOOST_ENDIAN_DEPRECATED_NAMES
#else
#include <boost/endian/endian.hpp>
#endif


class TestStorageFile;


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PhysicalStorage::StorageFile
    {
        friend class TestStorageFile;

        using RetCode = Storage::RetCode;
        using KeyCharT = typename Policies::KeyCharT;
        using ValueT = typename Storage::Value;
        using TimestampT = typename Storage::Timestamp;
        using OsPolicy = typename Policies::OSPolicy;
        using Handle = typename OsPolicy::HandleT;
        using big_uint64_t = boost::endian::big_uint64_t;
        using big_uint64_at = boost::endian::big_uint64_at;
        using CompatibilityStamp = big_uint64_t;
        
        inline static const auto InvalidHandle = OsPolicy::InvalidHandle;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;
        static constexpr auto ReaderNumber = Policies::PhysicalVolumePolicy::ReaderNumber;
        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static constexpr auto ChunkSize = Policies::PhysicalVolumePolicy::ChunkSize;


    public:

        using ChunkOffset = uint64_t;
        static constexpr ChunkOffset InvalidChunkOffset = std::numeric_limits< ChunkOffset >::max();

        class Transaction;
        class ostreambuf;
        class istreambuf;

    private:

        RetCode creation_status_ = RetCode::Ok;
        bool newly_created_ = false;

        std::string file_lock_name_;
        std::shared_ptr< boost::interprocess::named_mutex > file_lock_;

        mutable std::mutex transaction_mutex_;

        Handle writer_ = InvalidHandle;

        mutable std::mutex bloom_writer_mutex_;
        Handle bloom_writer_ = InvalidHandle;

        std::mutex readers_mutex_;
        boost::container::static_vector< Handle, ReaderNumber > readers_;
        std::stack< Handle, boost::container::static_vector< Handle, ReaderNumber > > reader_stack_;


        /** Rounds given value up by the module

        @tparam m - module
        @param [in] v - value to be rounded
        @return rounded value
        @throw nothing
        */
        template< size_t m >
        static constexpr auto round_up( size_t v ) noexcept
        {
            return m * ( v / m + ( v % m ? 1 : 0 ) );
        }


        /** Generates compatibility stamp

        @retval unique stamp of software settings
        @throw nothing
        */
        static CompatibilityStamp generate_compatibility_stamp() noexcept
        {
            return CompatibilityStamp{ variadic_hash( Key{}, ValueT{}, BloomSize, MaxTreeDepth, BTreeMinPower, ChunkSize ) };
        }


        /** Check software to file compatibility

        @throws nothing
        @warning  not thread safe
        */
        auto check_compatibility() noexcept
        {
            if ( std::get< bool> ( OsPolicy::seek_file( writer_, HeaderOffsets::of_CompatibilityStamp ) ) )
            {
                CompatibilityStamp stamp;

                if ( std::get< bool >( OsPolicy::read_file( writer_, &stamp, sizeof( stamp ) ) ) )
                {
                    if ( generate_compatibility_stamp() != stamp )
                    {
                        creation_status_ = RetCode::IncompatibleFile;
                    }
                }
                else
                {
                    creation_status_ = RetCode::IoError;
                }
            }
            else
            {
                creation_status_ = RetCode::IoError;
            }
        }

        static_assert( ChunkSize - 3 * sizeof( big_uint64_at ) > 0, "Chunk size too small" );
        static constexpr auto SpaceInChunk = ChunkSize - 3 * sizeof( big_uint64_at );

        struct chunk_t
        {
            big_uint64_at next_used_;
            big_uint64_at next_free_;
            big_uint64_at used_size_;
            std::array< uint8_t, SpaceInChunk > space_;
        };

        enum ChunkOffsets
        {
            of_NextUsed = offsetof( chunk_t, next_used_ ),
            sz_NextUsed = sizeof( chunk_t::next_used_ ),

            of_NextFree = offsetof( chunk_t, next_free_ ),
            sz_NextFree = sizeof( chunk_t::next_free_ ),

            of_UsedSize = offsetof( chunk_t, used_size_ ),
            sz_UsedSize = sizeof( chunk_t::used_size_ ),

            of_Space = offsetof( chunk_t, space_ ),
            sz_Space = sizeof( chunk_t::space_ ),
        };

        struct header_t
        {
            CompatibilityStamp compatibility_stamp;

            uint8_t bloom_[ BloomSize ];

            struct transactional_data_t
            {
                big_uint64_at file_size_;
                big_uint64_at free_space_;
            };

            transactional_data_t transactional_data_;

            transactional_data_t transaction_;

            big_uint64_at transaction_crc_;

            struct preserved_chunk_t
            {
                big_uint64_at target_;
                chunk_t chunk_;
            };

            preserved_chunk_t preserved_chunk_;
        };

        enum TransactionDataOffsets
        {
            of_FileSize = offsetof( header_t::transactional_data_t, file_size_ ),
            sz_FileSize = sizeof( header_t::transactional_data_t::file_size_ ),

            of_FreeSpace = offsetof( header_t::transactional_data_t, free_space_ ),
            sz_FreeSpace = sizeof( header_t::transactional_data_t::free_space_ ),
        };

        enum PreservedChunkOffsets
        {
            of_Target = offsetof( header_t::preserved_chunk_t, target_ ),
            sz_Target = sizeof( header_t::preserved_chunk_t::target_ ),

            of_Chunk = offsetof( header_t::preserved_chunk_t, chunk_ ),
            sz_Chunk = sizeof( header_t::preserved_chunk_t::chunk_ )
        };

        enum HeaderOffsets
        {
            // Compatibility
            of_CompatibilityStamp = offsetof( header_t, compatibility_stamp ),
            sz_CompatibilityStamp = sizeof( header_t::compatibility_stamp ),

            // Bloom filter
            of_Bloom = offsetof( header_t, bloom_ ),
            sz_Bloom = sizeof( header_t::bloom_ ),

            of_TransactionalData = offsetof( header_t, transactional_data_ ),
            sz_TransactionalData = sizeof( header_t::transactional_data_ ),

            of_Transaction = offsetof( header_t, transaction_ ),
            sz_Transaction = sizeof( header_t::transaction_ ),

            of_TransactionCrc = offsetof( header_t, transaction_crc_ ),
            sz_TransactionCrc = sizeof( header_t::transaction_crc_ ),

            of_PreservedChunk = offsetof( header_t, preserved_chunk_ ),
            sz_PreservedChunk = sizeof( header_t::preserved_chunk_ ),

            of_Root = sizeof( header_t )
        };


    public:

        static constexpr ChunkOffset RootChunkOffset = static_cast< ChunkOffset >( HeaderOffsets::of_Root );

    private:


        /** Initialize newly create file

        @throw nothing
        @warning not thread safe
        */
        auto deploy() noexcept
        {
            bool status = true;
            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            // write compatibility stamp
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_CompatibilityStamp ) );
            } );
            ce( [&] {
                CompatibilityStamp stamp = generate_compatibility_stamp();
                return std::get< bool >( OsPolicy::write_file( writer_, &stamp, sizeof( stamp ) ) );
            } );

            // write file size
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FileSize ) );
            } );
            ce( [&] {
                boost::endian::big_uint64_t value = HeaderOffsets::of_Root;
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate free space ptr
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_TransactionalData + TransactionDataOffsets::of_FreeSpace ) );
            } );
            ce( [&] {
                boost::endian::big_uint64_t value = InvalidChunkOffset;
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate transaction
            ce( [&] {
                return invalidate_transaction() == RetCode::Ok;
            } );

            return status ? RetCode::Ok : RetCode::IoError;
        }


        /** Invalidates current transaction

        @throw nothing
        @warning not thread safe
        */
        RetCode invalidate_transaction() const noexcept
        {
            using namespace std;

            bool status = true;
            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            header_t::transactional_data_t transaction;

            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_Transaction ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::read_file( writer_, &transaction, sizeof( transaction ) ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_TransactionCrc ) );
            } );
            ce( [&] {
                big_uint64_t trasaction_crc = variadic_hash( ( uint64_t )transaction.file_size_, ( uint64_t )transaction.free_space_ );
                return std::get< bool >( OsPolicy::write_file( writer_, &trasaction_crc, sizeof( trasaction_crc ) ) );
            } );

            return status ? RetCode::Ok : RetCode::IoError;
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

            if ( writer_ != InvalidHandle ) OsPolicy::close_file( writer_ );
            if ( bloom_writer_ != InvalidHandle ) OsPolicy::close_file( bloom_writer_ );

            assert( readers_.size() == ReaderNumber );
            for_each( execution::par, begin( readers_ ), end( readers_ ), [] ( auto h ) {
                if ( h != InvalidHandle ) OsPolicy::close_file( h ); 
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
        @param [in] create - create new if given file foes not exist
        @throw nothing
        */
        explicit StorageFile( const std::filesystem::path & path, bool create ) try
            : file_lock_name_{ "jb_lock_" + std::to_string( std::filesystem::hash_value( path ) ) }
        {
            using namespace std;

            // reserve and invalidate reader handles
            readers_.resize( ReaderNumber, InvalidHandle );
            reader_stack_ = move( std::stack< Handle, boost::container::static_vector< Handle, ReaderNumber > >{ readers_ } );

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
                creation_status_ = RetCode::AlreadyOpened;
            }

            // open writter
            if ( RetCode::Ok == creation_status_  )
            {
                if ( auto[ opened, tried_create, handle ] = OsPolicy::open_file( path, create ); !opened )
                {
                    creation_status_ = tried_create ? RetCode::UnableToCreate : RetCode::UnableToOpen;
                }
                else
                {
                    writer_ = handle;

                    // if newly created
                    if ( tried_create )
                    {
                        // initial deploy
                        deploy();

                        // notify storage
                        newly_created_ = true;
                    }
                    else
                    {
                        check_compatibility();
                    }
                }
            }

            // open Bloom writer
            if ( RetCode::Ok == creation_status_ )
            {
                if ( auto[ opened, tried_create, handle ] = OsPolicy::open_file( path, false ); !opened )
                {
                    creation_status_ = RetCode::UnableToOpen;
                }
                else
                {
                    bloom_writer_ = handle;
                }
            }

            // open readers
            for ( auto & reader : readers_ )
            {
                if ( RetCode::Ok != creation_status_ )
                {
                    break;
                }

                if ( auto[ opened, tried_create, handle ] = OsPolicy::open_file( path, false ); !opened )
                {
                    creation_status_ = RetCode::UnableToOpen;
                }
                else
                {
                    reader = handle;
                }
            }
        }
        catch ( const std::bad_alloc & )
        {
            creation_status_ = RetCode::InsufficientMemory;
        }
        catch ( ... )
        {
            creation_status_ = RetCode::UnknownError;
        }


        /** Provides creation status

        @throw nothing
        */
        auto creation_status() const noexcept { return creation_status_; }


        /** Let us know if the file is newly created

        @throw nothing
        */
        auto newly_created() const noexcept { return newly_created_; }


        /** Reads data for Bloom filter

        @param [out] bloom_buffer - target for Bloom data
        @return RetCode - status of operation
        @throw nothing
        @wraning not thread safe
        */
        RetCode read_bloom( uint8_t * bloom_buffer ) const noexcept
        {
            if ( !newly_created_ )
            {
                bool status = true;

                auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

                // seek & read data
                ce( [&] {
                    return std::get< bool >( OsPolicy::seek_file( bloom_writer_, HeaderOffsets::of_Bloom ) );
                } );
                ce( [&] {
                    return std::get< bool >( OsPolicy::read_file( bloom_writer_, bloom_buffer, BloomSize ) );
                } );

                return status ? RetCode::Ok : RetCode::IoError;
            }
            else
            {
                std::fill( std::execution::par, bloom_buffer, bloom_buffer + BloomSize, 0 );
                return RetCode::Ok;
            }
        }


        /** Write changes from Bloom filter

        @param [in] byte_no - ordinal number of byte in the array
        @param [in] byte - value to be written
        @retval - operation status
        @thrown nothing
        */
        RetCode add_bloom_digest( size_t byte_no, uint8_t byte ) const noexcept
        {
            using namespace std;

            assert( byte_no < BloomSize );

            scoped_lock l( bloom_writer_mutex_ );

            bool status = true;
            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            // seek & write data
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( bloom_writer_, HeaderOffsets::of_Bloom + byte_no ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::write_file( bloom_writer_, &byte, 1 ) );
            } );

            return status ? RetCode::Ok : RetCode::IoError;
        }


        /** Starts new transaction
        */
        std::tuple< RetCode, Transaction > open_transaction() const noexcept
        {
            using namespace std;

            Transaction transaction{ const_cast< Storage* >( this ), move( scoped_lock{ write_mutex } ) };

            // conditional execution
            bool status = true;
            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            // read transactional data: file size...
            boost::endian::big_uint64_t file_size;
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_FileSize ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::read_file( writer_, &file_size, sizeof( file_size ) ) );
            } );
            transaction.file_size_ = file_size;

            // ... free space
            boost::endian::big_uint64_t free_space;
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffsets::of_FreeSpace ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::read_file( writer_, &free_space, sizeof( free_space ) ) );
            } );


            return status ? std::tuple{ RetCode::Ok, }
        }
    };


    /** Represents transaction to storage file

    @tparam Policies - global setting
    @tparam Pad - test stuff
    */
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::PhysicalStorage::StorageFile::Transaction
    {
        friend class StorageFile;

        StorageFile * file_ = nullptr;
        std::scoped_lock< std::mutex > lock_;
        Handle handle_ = InvalidHandle;
        bool commited_ = false;
        uint64_t file_size_;
        ChunkOffset free_space_;
        ChunkOffset released_head_ = InvalidChunkOffset, released_tile_ = InvalidChunkOffset;
        bool status_ = false;


        /* Constructor

        @param [in] file - related file object
        @param [in] lock - write lock
        @throw nothing
        */
        explicit Transaction( StorageFile * file, Handle handle, std::scoped_lock< std::mutex > && lock ) noexcept try
            : file_{ file }
            , handle_{ handle }
            , lock_{ move( lock ) }
            , status_{ true }
        {
            assert( file_ );
            assert( hadle_ != InvalidHandle );
        }


    public:

        /** Default constructor, creates dummy transaction
        */
        Transaction() noexcept = default;


        /** The class is not copyable
        */
        Transaction( const Transaction & ) = delete;
        Transaction & operator = ( const Transaction & ) = delete;


        /** But movable
        */
        Transaction( Transaction&& ) noexcept = default;


        /** Marks a chain started from given chunk as released

        @param [in] chunk - staring chunk
        @retval operation status
        @throw nothing
        */
        RetCode erase_chain( ChunkOffset chunk ) noexcept
        {
            // conditional execution
            auto ce = [&] ( const auto & f ) noexcept { if ( status_ ) status_ = f(); };

            // check that given chunk is valid
            if ( chunk > file_size_ )
            {
                return RetCode::UnknownError;
            }

            // initialize pointers to released space end
            ce( [&] {
                released_tile_ = ( released_tile_ == InvalidChunkOffset ) ? chunk : released_tile_;
            } );

            while ( chunk != InvalidChunkOffset )
            {
                // get next used for current chunk
                big_uint64_t next_used;
                ce( [&] {
                    return std::get< bool >( OsPolicy::seek_file( handle_, chunk + chunk_t::of_NextUsed ) );
                } );
                ce( [&] {
                    return std::get< bool >( OsPolicy::read_file( handle_, &next_used, sizeof( next_used ) ) );
                } );

                // set next free to released head
                big_uint64_t next_free = released_head_;
                ce( [&] {
                    return std::get< bool >( OsPolicy::seek_file( handle_, chunk + chunk_t::of_NextFree ) );
                } );
                ce( [&] {
                    return std::get< bool >( OsPolicy::write_file( handle_, &next_free, sizeof( next_free ) ) );
                } );

                // go to next chunk
                chunk = next_used;
            }

            return status_ ? RetCode::Ok : RetCode::IoError;
        }

        /** Commit transaction

        @retval - operation status
        @throw nothing
        */
        auto commit() noexcept
        {
            if ( ! commited_ )
            {

            }
            else
            {
                return RetCode::UnknownError;
            }
        }
    };

}

#endif
