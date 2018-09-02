#ifndef __JB__BLOOM__H__
#define __JB__BLOOM__H__


#include <array>
#include <execution>
#include <boost/container/static_vector.hpp>


namespace jb
{
    template < typename Policies >
    class Storage< Policies >::PhysicalVolumeImpl::Bloom
    {
        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using StorageFile = typename PhysicalVolumeImpl::StorageFile;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto BloomFnCount = Policies::PhysicalVolumePolicy::MaxTreeDepth;

        static_assert( BloomSize % sizeof( uint64_t ) == 0, "Invalid Bloom size" );
        static_assert( BloomFnCount > 0, "Invalid tree depth" );

        std::atomic< RetCode > status_ = RetCode::Ok;
        StorageFile & file_;
        std::array< uint8_t, BloomSize > alignas( sizeof( uint64_t ) ) filter_;

        auto set_status( RetCode status ) noexcept
        {
            auto ok = RetCode::Ok;
            status_.compare_exchange_weak( ok, status, std::memory_order_acq_rel, std::memory_order_relaxed );
        }


    public:

        /** Declaration of single digest that is actually just hash value of a key segment
        */
        using Digest = uint32_t;

        struct bloom_error : public std::runtime_error
        {
            bloom_error( RetCode rc, const char * what ) : std::runtime_error( what ), rc_( rc ) {}
            RetCode code() const noexcept { return rc_; }
        private:
            RetCode rc_;
        };

        //using DigestPath = boost::container::static_vector< Digest, BloomFnCount >;
        using DigestPath = std::vector< Digest >;

        /** No default constructible/copyable/movable
        */
        Bloom() = delete;
        Bloom( Bloom && ) = delete;


        /** Constructor

        @param [in] storage - associated physical storage
        */
        explicit Bloom( StorageFile & file ) try : file_( file )
        {
            if ( RetCode::Ok == file_.status() )
            {
                file_.read_bloom( filter_.data() );
            }
        }
        catch ( const storage_file_error & e )
        {
            set_status( e.code() );
        }
        catch ( ... )
        {
            set_status( RetCode::UnknownError );
        }


        [ [ nodiscard ] ]
        auto status() const noexcept { return status_.load( std::memory_order_acquire ); }


        /** Generate digest for a key considering level and stem

        @param [in] level - key level
        @param [in] key - stem of key's segment
        @retval key's digest
        */
        static Digest generate_digest( size_t level, const Key & key )
        {
            if ( level >= BloomFnCount )
            {
                throw bloom_error( RetCode::MaxTreeDepthExceeded, "" );
            }

            if ( !key.is_leaf() )
            {
                throw bloom_error( RetCode::InvalidSubkey, "" );
            }

            return variadic_hash( level, key ) & std::numeric_limits< uint32_t >::max();
        }


        /** Updates the filter adding another segment digest

        @param [in] digest - digest to be added
        @throw nothing
        */
        auto add_digest( Digest digest )
        {
            using namespace std;

            const auto byte_no = ( digest / 8 ) % BloomSize;
            const auto bit_no = digest % 8;

            // update memory and file
            static mutex guard;
            scoped_lock lock( guard );

            filter_[ byte_no ] |= ( 1 << bit_no );
            file_.add_bloom_digest( byte_no, filter_[ byte_no ] );
        }


        /** Checks if combination of given keys MAY present

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        @retval RetCode - operation status
        @retval size_t - number of generated digest
        @retval bool - if combined key may present
        @throw nothing
        */
        bool test( const Key & prefix, const Key & suffix, DigestPath & digests ) const
        {
            using namespace std;

            digests.clear();

            if ( !prefix.is_path() && !suffix.is_path() )
            {
                throw bloom_error( RetCode::UnknownError, "Bad path" );
            }

            size_t level = 1;

            auto get_digests = [&] ( const auto & key ) {
                
                if ( Key::root() != key )
                {
                    auto rest = key;

                    while ( rest.size() )
                    {
                        if ( digests.size() >= BloomFnCount )
                        {
                            throw bloom_error( RetCode::MaxTreeDepthExceeded, "" );
                        }

                        auto[ split_ok, prefix, suffix ] = rest.split_at_head();
                        auto[ trunc_ok, stem ] = prefix.cut_lead_separator();
                        auto digest = generate_digest( level, stem );

                        digests.push_back( digest );

                        rest = suffix;
                        level++;
                    }
                }
            };

            get_digests( prefix );
            get_digests( suffix );

            if ( !digests.size() ) return true;

            auto may_present = true;

            for ( auto digest : digests )
            {
                const auto byte_no = ( digest / 8 ) % BloomSize;
                const auto bit_no = digest % 8;
                    
                auto check = filter_[ byte_no ] & ( 1 << bit_no );

                if ( check == 0 )
                {
                    may_present = false;
                    break;
                }
            };

            return may_present;
        }
    };
}

#endif
