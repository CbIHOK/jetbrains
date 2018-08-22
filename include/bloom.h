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
        using PhysicalStorage = typename PhysicalVolumeImpl::PhysicalStorage;

        static constexpr auto BloomSize = Policies::PhysicalVolumePolicy::BloomSize;
        static constexpr auto BloomFnCount = Policies::PhysicalVolumePolicy::MaxTreeDepth;

        static bool constexpr power_of_2( size_t value )
        {
            return value && ( value & ( value - 1 ) ) == 0;
        }

        static_assert( power_of_2( BloomSize ), "Should be power of 2" );
        static_assert( BloomFnCount > 0, "Invalid number of Bloom functions" );

        RetCode creation_status_ = RetCode::Ok;
        PhysicalStorage * storage_ = nullptr;
        std::array< uint8_t, BloomSize > filter_;


    public:

        using Digest = size_t;

        Bloom() = delete;
        Bloom( Bloom && ) = delete;

        explicit Bloom( PhysicalStorage * storage = nullptr ) try : storage_( storage )
        {
            if ( storage_ && RetCode::Ok == storage_->creation_status() )
            {
                creation_status_ = storage_->read_bloom( filter_.data() );
            }

            filter_.fill( 0 );
        }
        catch ( ... )
        {
            creation_status_ = RetCode::UnknownError;
        }


        auto creation_status() const noexcept { return creation_status_; }


        /** Updates the filter adding another combinations of keys

        @param [in] prefix - prefix key
        @param [in] suffix - suffix key
        */
        auto add_digest( Digest digest ) noexcept
        {
            using namespace std;

            const auto byte_no = ( digest / 8 ) % BloomSize;
            const auto bit_no = digest % 8;
            filter_[ byte_no ] |= ( 1 << bit_no );

            if ( storage_ && RetCode::Ok == ->creation_status() )
            {
                return storage_->update_bloom( byte_no, filter_[ byte_no ] );
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

                        auto digest = Hash< Key >{}( stem );
                        digests.push_back( digest );

                        rest = suffix;
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
                    
                    if ( ( filter_[ byte_no ] & ( 1 << bit_no ) ) == 0 )
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
