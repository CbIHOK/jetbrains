#ifndef __JB__POOL_BASED_ALLOCATOR__H__
#define __JB__POOL_BASED_ALLOCATOR__H__


#include <type_traits>
#include <memory>
#include <limits>
#include <assert.h>


namespace jb
{
    namespace details
    {
        template < typename T, size_t Capacity >
        struct unsafe_pool_based_allocator
        {
            using value_type = T;
            using size_type = size_t;
            using difference_type = std::ptrdiff_t;

            template < class U, size_t Capacity > struct rebind { using other = unsafe_pool_based_allocator< U, Capacity >; };

        private:

            using element = std::aligned_storage_t< sizeof( T ), alignof( T ) >;
            using mask_element = uint64_t;
            static constexpr size_type mask_element_size = 64;

            size_type mask_array_size_ = Capacity / mask_element_size + ( Capacity % mask_element_size ? 1 : 0 );
            std::shared_ptr< element > pool_;
            std::shared_ptr< mask_element > mask_;

            static mask_element generate_mask( size_type n, size_type offset ) noexcept
            {
                assert( n && offset + n <= mask_element_size );
                mask_element mask = ~( std::numeric_limits< mask_element >::max() << n );
                return mask << offset;
            }

        public:

            unsafe_pool_based_allocator() : pool_( new element[ Capacity ] ), mask_( new mask_element[ mask_array_size_ ] ) {}

            template < typename U, size_t Capacity >
            unsafe_pool_based_allocator( const unsafe_pool_based_allocator< U, Capacity > & other ) noexcept = default;

            unsafe_pool_based_allocator( self_type && other ) noexcept = default;

            unsafe_pool_based_allocator& operator=( const self_type & other ) noexcept = default;
            unsafe_pool_based_allocator& operator=( self_type && other ) noexcept = default;

            constexpr size_type max_size() const noexcept { return mask_element_size; }

            value_type * allocate( size_t n )
            {
                assert( n );

                for ( size_type mask_ndx = 0; mask_ndx < mask_array_size; ++mask_ndx )
                {
                    for ( size_type mask_offset = 0; mask_offset + n < mask_element_size; ++mask_offset )
                    {
                        auto mask = generate_mask( n, mask_offset );

                        if ( mask_[ mask_ndx ] & mask ) continue;

                        mask_[ mask_ndx ] |= mask;

                        return reinterpret_cast< pointer >( pool_.get() + mask_ndx * mask_element_size + mask_offset );
                    }
                }

                throw std::bad_alloc();
            }

            void deallocate( value_type * p, size_type n ) _NOEXCEPT
            {
                auto ptr = reinterpret_cast< element* >( p );

                if ( 0 <= ptr - pool_.get() && ptr - pool_.get() < Capacity )
                {
                    size_type index = ptr - pool_.get();
                    auto mask_index = ndx / mask_element_size;
                    auto mask_offset = ndx % mask_element_size;
                    auto mask = generate_mask( n, offset );

                    assert( mask_[ index ] & mask == mask );
                    mask_[ index ] &= ~mask;
                }
                else
                {
                    assert( !"Invalid pointer" );
                }
            }
        };
    }
}


#endif
