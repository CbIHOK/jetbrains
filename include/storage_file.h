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

#include "variadic_hash.h"


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
        using CompatibilityStamp = uint64_t;
        using BloomDigest = typename Storage::PhysicalVolumeImpl::Bloom::Digest;
        
        inline static const auto InvalidHandle = OsPolicy::InvalidHandle;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto MaxTreeDepth = Policies::PhysicalVolumePolicy::MaxTreeDepth;
        static constexpr auto ReaderNumber = Policies::PhysicalVolumePolicy::ReaderNumber;
        static constexpr auto BTreeMinPower = Policies::PhysicalVolumePolicy::BTreeMinPower;
        static constexpr auto ChunkSize = Policies::PhysicalVolumePolicy::ChunkSize;


    public:

        using NodeUid = uint64_t;
        static constexpr auto InvalidNodeUid = std::numeric_limits< NodeUid >::max();

    private:

        RetCode creation_status_ = RetCode::Ok;
        bool newly_created_ = false;

        std::string file_lock_name_;
        std::shared_ptr< boost::interprocess::named_mutex > file_lock_;

        Handle writer_ = InvalidHandle;

        std::mutex readers_mutex_;
        boost::container::static_vector< Handle, ReaderNumber > readers_;
        std::stack< Handle, boost::container::static_vector< Handle, ReaderNumber > > reader_stack_;


        static constexpr auto little_endian() noexcept
        {
            static constexpr auto tester = 1U;
            return *( reinterpret_cast< const uint8_t* >( &tester ) ) == 1;
        }


        template< size_t m >
        static constexpr auto round_up( size_t v ) noexcept
        {
            return m * ( v / m + ( v % m ? 1 : 0 ) );
        }


        template < typename T >
        static T normalize( T v ) noexcept
        {
            static_assert( std::is_integral<T>::value, "Only integral types are allowed" );

            if ( little_endian() )
            {
                T v_;

                for ( uint8_t *s = reinterpret_cast< uint8_t* >( &v ), *d = reinterpret_cast< uint8_t* >( &v_ ) + sizeof( T ) - 1;
                    s < reinterpret_cast< uint8_t* >( &v ) + sizeof( T );
                    ++s, --d )
                {
                    *d = *s;
                }

                return v_;
            }
            else
            {
                return v;
            }
        }


        static CompatibilityStamp generate_compatibility_stamp() noexcept
        {
            return variadic_hash( KeyCharT{}, ValueT{}, BloomSize, MaxTreeDepth, BTreeMinPower, ChunkSize );
        }


        auto check_compatibility() noexcept
        {
            if ( std::get< bool> ( OsPolicy::seek_file( writer_, ( int64_t )Offset::of_CompatibilityStamp, OsPolicy::SeekMethod::Begin ) ) )
            {
                CompatibilityStamp stamp;

                if ( std::get< bool >( OsPolicy::read_file( writer_, &stamp, sizeof( stamp ) ) ) )
                {
                    if ( normalize( generate_compatibility_stamp() ) != stamp )
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


        enum class Offset
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

            of_Checksum_1 = of_FreeSpacePtrCopy + sz_FreeSpacePtrCopy,
            sz_Checksum_1 = sizeof( uint64_t ),

            of_PreservedChunk = round_up< SectorSize >( of_Checksum_1 + sz_Checksum_1 ),
            sz_PreservedChunk = ChunkSize,

            of_Checksum_2 = of_PreservedChunk + sz_PreservedChunk,
            sz_Checksum_2 = sizeof( uint64_t ),

            of_Root = round_up< SectorSize >( of_Checksum_2 + sz_Checksum_2 )
        };


        /** Initialize newly create file

        @throw nothing
        */
        auto deploy() noexcept
        {
            bool status = true;

            auto ce = [&] ( const auto & f ) noexcept { if ( status ) status = f(); };

            // write compatibility stamp
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, ( int64_t )Offset::of_CompatibilityStamp, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                CompatibilityStamp stamp = normalize( generate_compatibility_stamp() );
                return std::get< bool >( OsPolicy::write_file( writer_, &stamp, sizeof( stamp ) ) );
            } );

            // write file size
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, ( int64_t )Offset::of_FileSize, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                auto value = normalize( ( uint64_t )Offset::of_Root );
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate free space ptr
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, ( int64_t )Offset::of_FreeSpacePtr, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                auto value = normalize( InvalidNodeUid );
                return std::get< bool >( OsPolicy::write_file( writer_, &value, sizeof( value ) ) );
            } );

            // invalidate ( file size, free space ptr ) copy
            ce( [&] {
                return std::get< bool >( OsPolicy::seek_file( writer_, ( int64_t )Offset::of_Checksum_1, OsPolicy::SeekMethod::Begin ) );
            } );
            ce( [&] {
                auto value = normalize( variadic_hash( ( uint64_t )Offset::of_Root, InvalidNodeUid ) + 1 );
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

            assert( readers_.size() == ReaderNumber );
            if ( writer_ != InvalidHandle ) OsPolicy::close_file( writer_ );
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
        */
        RetCode read_bloom( uint8_t * bloom_buffer ) const noexcept
        {
            return RetCode::NotImplementedYet;
        }


        /** Write changes from Bloom filter
        */
        std::tuple< RetCode > add_bloom_digest( const BloomDigest & digest ) const noexcept
        {
            using namespace std;

            auto ret = RetCode::Ok;

            for_each( begin( digest ), begin( digest ) + BloomFnCount, [&] ( auto & fn ) {
                auto byte_no = fn / 8;
                auto bit_no = fn % 8;

                if (  )e
            } );
        }
    };
}

#endif
