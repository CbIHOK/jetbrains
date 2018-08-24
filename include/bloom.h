#ifndef __JB__BLOOM__H__
#define __JB__BLOOM__H__


#include <array>
#include <atomic>
#include <execution>
#include <boost/container/static_vector.hpp>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::PhysicalVolumeImpl::Bloom
    {
        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using StorageFile = typename PhysicalVolumeImpl::StorageFile;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto BloomFnCount = Policies::PhysicalVolumePolicy::MaxTreeDepth;

        static bool constexpr power_of_2( size_t value )
        {
            return value && ( value & ( value - 1 ) ) == 0;
        }

        static_assert( power_of_2( BloomSize ), "Should be power of 2" );
        static_assert( BloomFnCount > 0, "Invalid number of Bloom functions" );

        RetCode status_ = RetCode::Ok;
        StorageFile * file_ = nullptr;
        std::array< uint8_t, BloomSize > filter_;
        mutable std::atomic_flag lock_ = ATOMIC_FLAG_INIT;


    public:

        /** No default constructible/copyable/movable
        */
        Bloom() = delete;
        Bloom( Bloom && ) = delete;


        /** Constructor

        @param [in] storage - associated physical storage
        */
        explicit Bloom( StorageFile * file = nullptr ) try : file_( file )
        {
            if ( file_ && RetCode::Ok == file_->creation_status() )
            {
                status_ = file_->read_bloom( filter_.data() );
            }
            else
            {
                std::fill( std::execution::par, filter_.data(), filter_.data() + filter_.size(), 0 );
            }
        }
        catch ( ... )
        {
            status_ = RetCode::UnknownError;
        }


        /** Provides creation status

        @retval - creation status
        @throw nothing
        */
        auto status() const noexcept { return status_; }


        /** Declaration of single digest that is actually just hash value of a key segment
        */
        using Digest = size_t;


        /** Generate digest for a key considering level and stem

        @param [in] level - key level
        @param [in] key - stem of key's segment
        @retval key's digest
        */
        static Digest generate_digest( size_t level, const Key & key ) noexcept
        {
            assert( level < BloomFnCount );
            assert( key.is_leaf() );
            return variadic_hash( level, key );
        }


        /** Updates the filter adding another segment digest

        @param [in] digest - digest to be added
        @throw nothing
        */
        RetCode add_digest( Digest digest ) noexcept
        {
            using namespace std;

            const auto byte_no = ( digest / 8 ) % BloomSize;
            const auto bit_no = digest % 8;

            // update memory under spinlock
            while ( lock_.test_and_set( std::memory_order_acquire ) );
            filter_[ byte_no ] |= ( 1 << bit_no );
            lock_.clear( std::memory_order_release );

            if ( file_ && RetCode::Ok == file_->creation_status() )
            {
                return file_->add_bloom_digest( byte_no, filter_[ byte_no ] ) ;
            }

            return RetCode::Ok;
        }


        /** Checks if combination of given keys MAY present

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        @retval RetCode - operation status
        @retval size_t - number of generated digest
        @retval bool - if combined key may present
        @throw nothing
        */
        std::tuple< RetCode, size_t, bool > test( const Key & prefix, const Key & suffix ) const noexcept
        {
            using namespace std;

            assert( prefix.is_path() && suffix.is_path() );

            boost::container::static_vector< Digest, BloomFnCount > digests;
            auto status = RetCode::Ok;

            size_t level = 0;

            auto get_digests = [&] ( const auto & key ) noexcept {
                
                if ( Key::root() != key )
                {
                    auto rest = key;

                    while ( rest.size() && RetCode::Ok == status )
                    {
                        if ( digests.size() == digests.capacity() )
                        {
                            status = RetCode::MaxTreeDepthExceeded;
                            break;
                        }

                        auto[ split_ok, prefix, suffix ] = rest.split_at_head();
                        assert( split_ok );

                        auto[ trunc_ok, stem ] = prefix.cut_lead_separator();
                        assert( trunc_ok );

                        auto digest = generate_digest( level, stem );
                        digests.push_back( digest );

                        rest = suffix;
                        level++;
                    }
                }
            };

            get_digests( prefix );
            get_digests( suffix );

            if ( RetCode::Ok == status )
            {
                bool result = true;

                for ( auto digest : digests )
                {
                    const auto byte_no = ( digest / 8 ) % BloomSize;
                    const auto bit_no = digest % 8;
                    
                    while ( lock_.test_and_set( std::memory_order_acquire ) );
                    auto check = filter_[ byte_no ] & ( 1 << bit_no );
                    lock_.clear( std::memory_order_release );

                    if ( check == 0 )
                    {
                        result = false;
                        break;
                    }
                };

                return { status, digests.size(), result };
            }

            return { status, digests.size(), false };
        }
    };
}

#endif
