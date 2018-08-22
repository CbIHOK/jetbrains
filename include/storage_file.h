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
        using CompatibilityStamp = boost::endian::big_uint64_t;
        
        inline static const auto InvalidHandle = OsPolicy::InvalidHandle;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;
        static constexpr auto ReaderNumber = Policies::PhysicalVolumePolicy::ReaderNumber;
        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static constexpr auto ChunkSize = Policies::PhysicalVolumePolicy::ChunkSize;


    public:

        using NodeUid = uint64_t;
        static constexpr NodeUid InvalidNodeUid = std::numeric_limits< NodeUid >::max();

    private:

        RetCode creation_status_ = RetCode::Ok;
        bool newly_created_ = false;

        std::string file_lock_name_;
        std::shared_ptr< boost::interprocess::named_mutex > file_lock_;

        mutable std::mutex writer_mutex_;
        Handle writer_ = InvalidHandle;

        mutable std::mutex bloom_writer_mutex_;
        Handle bloom_writer_ = InvalidHandle;

        std::mutex readers_mutex_;
        boost::container::static_vector< Handle, ReaderNumber > readers_;
        std::stack< Handle, boost::container::static_vector< Handle, ReaderNumber > > reader_stack_;


        ///** Let us know if the system uses Little Endian

        //@retval true if the system uses Little Endian
        //@throw nothing
        //*/
        //static constexpr auto little_endian() noexcept
        //{
        //    static constexpr auto tester = 1U;
        //    return *( reinterpret_cast< const uint8_t* >( &tester ) ) == 1;
        //}


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


        ///** If the sysytem uses Little Endian converts given value to Big Endian

        //else returns the value untouched

        //TODO: consider using specialized C++ functions

        //@tparam T - integral type
        //@param [in] v - value
        //@retval the value in Big Endian form
        //@throw nothing
        //*/
        //template < typename T >
        //static T normalize( T v ) noexcept
        //{
        //    static_assert( std::is_integral<T>::value, "Only integral types are allowed" );

        //    if ( little_endian() )
        //    {
        //        T v_;

        //        for ( uint8_t *s = reinterpret_cast< uint8_t* >( &v ), *d = reinterpret_cast< uint8_t* >( &v_ ) + sizeof( T ) - 1;
        //            s < reinterpret_cast< uint8_t* >( &v ) + sizeof( T );
        //            ++s, --d )
        //        {
        //            std::swap( *d, *s );
        //        }

        //        return v_;
        //    }
        //    else
        //    {
        //        return v;
        //    }
        //}


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
            if ( std::get< bool> ( OsPolicy::seek_file( writer_, HeaderOffset::of_CompatibilityStamp, OsPolicy::SeekMethod::Begin ) ) )
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


        /** Enumerates predefined offsets in data file
        */
        enum HeaderOffset
        {
            of_CompatibilityStamp = 0,
            sz_CompatibilityStamp = sizeof( CompatibilityStamp ),

            of_Bloom = of_CompatibilityStamp + sz_CompatibilityStamp,
            sz_Bloom = BloomSize,

            SectorSize = 4096,

            of_FileSize = round_up< SectorSize >( of_Bloom + sz_Bloom ),
            sz_FileSize = sizeof( uint64_t ),

            of_FreeSpacePtr = of_FileSize + sz_FileSize,
            sz_FreeSpacePtr = sizeof( NodeUid ),

            of_FileSizeCopy = round_up< SectorSize >( of_FreeSpacePtr + sz_FreeSpacePtr ),
            sz_FileSizeCopy = sz_FileSize,

            of_FreeSpacePtrCopy = of_FileSizeCopy + sz_FileSizeCopy,
            sz_FreeSpacePtrCopy = sz_FreeSpacePtr,

            of_PreservedChunkUid = of_FreeSpacePtrCopy + sz_FreeSpacePtrCopy,
            sz_PreservedChunkUid = sizeof( NodeUid ),

            of_PreservedChunk = of_PreservedChunkUid + sz_PreservedChunkUid,
            sz_PreservedChunk = ChunkSize,

            of_Checksum = of_PreservedChunk + sz_PreservedChunk,
            sz_Checksum = sizeof( uint64_t ),

            of_Transaction = of_FileSizeCopy,
            sz_Transaction = of_Checksum - of_FileSizeCopy,

            of_Root = round_up< SectorSize >( of_Checksum + sz_Checksum ),
        };


    public:

        static constexpr NodeUid RootNodeUid = static_cast< NodeUid >( HeaderOffset::of_Root );

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
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffset::of_CompatibilityStamp, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                CompatibilityStamp stamp = generate_compatibility_stamp();
                return std::get< bool >( OsPolicy::write_file( writer_, &stamp, sizeof( stamp ) ) );
            } );

            // write file size
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffset::of_FileSize, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                boost::endian::big_uint64_t value = HeaderOffset::of_Root;
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate free space ptr
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffset::of_FreeSpacePtr, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                boost::endian::big_uint64_t value = InvalidNodeUid;
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate transaction
            using transaction_t = array< uint8_t, HeaderOffset::sz_Transaction >;
            transaction_t transaction;
            transaction.fill( 0 );
            uint64_t transaction_hash = boost::hash< transaction_t >{}( transaction );

            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffset::of_Checksum, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                boost::endian::big_uint64_t value = transaction_hash + 1;
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
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
                    return std::get< bool >( OsPolicy::seek_file( bloom_writer_, HeaderOffset::of_Bloom, OsPolicy::SeekMethod::Begin ) );
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
        */
        std::tuple< RetCode > add_bloom_digest( size_t byte_no, uint8_t byte ) const noexcept
        {
            using namespace std;

            assert( byte_no < BloomSize );

            scoped_lock l( bloom_writer_mutex_ );

            bool status = true;

            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            // seek & read data
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, HeaderOffset::of_Bloom + byte_no , OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                return std::get< bool >( OsPolicy::write_file( writer_, &byte, 1 ) );
            } );

            return status ? RetCode::Ok : RetCode::IoError;
        }
    };
}

#endif
