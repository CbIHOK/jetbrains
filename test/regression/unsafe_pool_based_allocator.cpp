#include <gtest/gtest.h>
#include <details/unsafe_pool_based_allocator.h>
#include <unordered_set>
#include <list>


template < typename T >
struct unsafe_pool_based_allocator_test : public ::testing::Test
{
    using element_t = T;
    using my_allocator = jb::details::unsafe_pool_based_allocator< T >;
};

typedef ::testing::Types<
    uint64_t
> TestingPolicies;


TYPED_TEST_CASE( unsafe_pool_based_allocator_test, TestingPolicies );


TYPED_TEST( unsafe_pool_based_allocator_test, unordered_multiset )
{
    std::unordered_multiset< element_t, std::hash< element_t >, std::equal_to< element_t >, my_allocator > tmp;
    EXPECT_TRUE( true );
}