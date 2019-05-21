#ifndef __JB__RARE_WRITE_FREQUENT_READ_MUTEX__H__
#define __JB__RARE_WRITE_FREQUENT_READ_MUTEX__H__


#include <atomic>
#include <thread>
#include <memory>
#include <new>
#include <functional>


namespace jb
{
    namespace details
    {
        /** Implements a shared mutex concept optimized for a case of frequent shared (READ) but rare exclusive (WRITE)

        locks. The main idea is to reduce the probability of moving cache line to SHARED state upon getting a shared lock.
        The implementation replaces single atomic representing shared lock with whole array of them, and a thread selects
        certain one by their ID. That improves the probablity that when a thread tries to get shared lock corresponding
        cache line stays in EXCUSIVE/MODIFIED state and CPU does not spend additional time for cache synchronization. The
        payback is higher impact in the case of EXCLUSIVE lock cuz it must make sure that ALL shared locks are released.
        Another disadvantage that such mutex cannot prevent recursive shared locks, but an attempt to upgrade over recursive
        lock causes deadlock.

        @tparam SharedLockHasher - number of atomics representing shared locks
        */
        template < size_t SharedLockHasher = 15 >
        class rare_write_frequent_read_mutex
        {
            static_assert( SharedLockHasher );

            //
            // atomic integer aligned onto CPU cache to avoid any possible interference between 
            //
            struct alignas( std::hardware_destructive_interference_size ) aligned_atomic_t
            {
                std::atomic_uint32_t atomic_;
            };


            static constexpr uint32_t unlocked = 0;
            static constexpr uint32_t locked = 1;


            //
            // EXCLUSIVE lock
            //
            aligned_atomic_t x_lock_;

            //
            // SHARED locks
            //
            aligned_atomic_t s_locks_[ SharedLockHasher ];


            /** Gets EXCLUSIVE lock in context of calling thread

            @tparam SpinCount - try count before calling thread let's other threads pass
            @throw nothing
            */
            template < size_t SpinCount >
            void lock() _NOEXCEPT
            {
                static_assert( SpinCount );

                while ( true )
                {
                    // get unique lock
                    for ( size_t try_count = 1;; ++try_count )
                    {

                        // try get the lock
                        uint32_t expected = unlocked;
                        if ( x_lock_.atomic_.compare_exchange_weak( expected, locked, std::memory_order_acq_rel, std::memory_order_relaxed ) )
                        {
                            break;
                        }

                        // if spin count exceeded - yield to other threads
                        if ( try_count % SpinCount == 0 )
                        {
                            std::this_thread::yield();
                        }
                    }

                    bool ok = true;

                    // for all shared locks
                    for ( auto & s_lock : s_locks_  )
                    {
                        // wait until a shared lock gets released
                        for ( size_t try_count = 1; ; ++try_count )
                        {
                            //
                            // we do not need to use ACQUIRE semantic cuz another shared lock cannot be taken. The question still is
                            // about performance, if repeating processing of the invalidation queue 
                            if ( !s_lock.atomic_.load( std::memory_order_acquire ) )
                            {
                                break;
                            }

                            // if spin count exceeded 
                            if ( try_count % SpinCount == 0 )
                            {
                                // unable to get exclusive lock, possible reason is upgrade_lock 
                                ok = false;
                                break;
                            }
                        }
                    }

                    // if all shared locks are released
                    if ( ok )
                    {
                        // done
                        break;
                    }
                    else
                    {
                        // release exclusive lock for awhile and try again
                        x_lock_.atomic_.store( unlocked, std::memory_order_release );
                    }
                }
            }


            /** Release EXCLUSIVE lock in context of calling thread

            @throw nothing
            */
            void unlock() _NOEXCEPT
            {
                // simply release unique lock
                x_lock_.atomic_.store( unlocked, std::memory_order_release );
            }


            /** Gets SHARED lock in context of calling thread

            @tparam SpinCount - try count before calling thread let's other threads pass
            @throw nothing
            */
            template < size_t SpinCount >
            void lock_shared() _NOEXCEPT
            {
                static_assert( SpinCount );

                const auto s_lock_ndx = std::hash< std::thread::id >{}( std::this_thread::get_id() ) % SharedLockHasher;
                auto & s_lock = s_locks_[ s_lock_ndx ];

                for ( size_t try_count = 1; ; ++try_count )
                {
                    // get temporary shared lock
                    s_lock.atomic_.fetch_add( locked, std::memory_order_acq_rel );

                    // if there is not exclusive lock request
                    if ( !x_lock_.atomic_.load( std::memory_order_acquire ) )
                    {
                        // convert temporary lock to permanent
                        break;
                    }

                    // release temporary lock
                    s_lock.atomic_.fetch_sub( locked, std::memory_order_acq_rel );

                    // if try count exceeded - yield other threads
                    if ( try_count % SpinCount == 0 )
                    {
                        std::this_thread::yield();
                    }
                }
            }


            /** Release SHARED lock in context of calling thread

            @throw nothing
            */
            void unlock_shared() _NOEXCEPT
            {
                // release shared lock
                const auto s_lock_ndx = std::hash< std::thread::id >{}( std::this_thread::get_id() ) % SharedLockHasher;
                auto & s_lock = s_locks_[ s_lock_ndx ];
                s_lock.atomic_.fetch_sub( locked, std::memory_order::memory_order_acq_rel );
            }


            /** Upgrades existing SHARED lock to EXCLUSIVE

            @tparam SpinCount - try count before thread yeild
            @throw nothing
            */
            template < size_t SpinCount >
            void lock_upgrade()
            {
                static_assert( SpinCount );

                const auto s_lock_ndx = std::hash< std::thread::id >{}( std::this_thread::get_id() ) % SharedLockHasher;

                // get exclusive lock
                for ( size_t try_count = 1;; ++try_count )
                {
                    // try to get the lock
                    uint32_t expected = 0;
                    if ( x_lock_.atomic_.compare_exchange_weak( expected, locked, std::memory_order_acq_rel, std::memory_order_relaxed ) )
                    {
                        break;
                    }

                    // if spin count exceeded - yield to other threads
                    if ( try_count % SpinCount == 0 )
                    {
                        std::this_thread::yield();
                    }
                }

                // for all shared locks
                for ( size_t lock_ndx = 0; lock_ndx < SharedLockHasher; ++lock_ndx )
                {
                    auto & s_lock = s_locks_[ lock_ndx ];

                    for ( size_t try_count = 1; ; ++try_count )
                    {
                        // wait until a shared lock gets released
                        auto lock_count = s_lock.atomic_.load( std::memory_order_acquire );
                        if ( !lock_count || locked == lock_count && lock_ndx == s_lock_ndx )
                        {
                            break;
                        }

                        // if spin count exceeded - yield to other threads
                        if ( try_count % SpinCount == 0 )
                        {
                            std::this_thread::yield();
                        }
                    }
                }
            }

        public:

            /** Implements scoped SHARED lock

            @tparam
            */
            template < size_t SpinCount = 1024 >
            class shared_lock
            {
                //
                // upgdare lock need access to mutex pointer
                //
                template < size_t SpinCount > friend class upgrade_lock;

                //
                // locked mutex pointer
                //
                rare_write_frequent_read_mutex * mutex_ = std::nullptr;

            public:

                /** Default constructor, does not take a lock

                @throw nothing
                */
                shared_lock() _NOEXCEPT {}


                /** Locking constructor, takes SHARED lock over goiven mutex

                @throw nothing
                */
                shared_lock( rare_write_frequent_read_mutex & mutex ) _NOEXCEPT : mutex_( &mutex )
                {
                    assert( mutex_ );
                    mutex_->lock_shared< SpinCount >();
                }


                /** The class is not copyable
                */
                shared_lock( const shared_lock & ) = delete;
                const shared_lock & operator = ( const shared_lock & ) = delete;


                /** Move constructor/assignment

                @throw nothing
                */
                template < size_t SpinCount >
                shared_lock( shared_lock< SpinCount > && origin ) _NOEXCEPT
                {
                    swap( origin );
                }

                template < size_t SpinCount >
                shared_lock & operator = ( shared_lock< SpinCount > && rhs ) _NOEXCEPT
                {
                    mutex_ = std::nullptr;
                    swap( rhs );
                    return *this;
                }


                /** Swaps with another other

                @param [in/out] other - instance to be swapped
                @throw nothing
                */
                template < size_t SpinCount >
                void swap( shared_lock< SpinCount> & other ) _NOEXCEPT
                {
                    std::swap( mutex_, other.mutex_ );
                }


                /** Destructor, release SHARED lock on related mutex

                @throw nothing
                */
                virtual ~shared_lock() _NOEXCEPT
                {
                    if ( mutex_ ) mutex_->unlock_shared();
                }
            };


            /** Scoped EXCUSIVE lock
            */
            template < size_t SpinCount = 1024 >
            class unique_lock
            {
                //
                // pointer to locked mutex
                //
                rare_write_frequent_read_mutex * mutex_ = std::nullptr;

            public:

                /** Default constructor, instanciate class without taking a lock

                @throw nothing
                */
                unique_lock() _NOEXCEPT {}


                /** Locking constructor, takes EXCLUSIVE lock over given mutex

                @param [in/out] mutex - mutex to be locked
                @throw nothing
                */
                unique_lock( rare_write_frequent_read_mutex & mutex ) _NOEXCEPT : mutex_( &mutex )
                {
                    assert( mutex_ );
                    mutex_->lock< SpinCount >();
                }


                /** Class is not copyable
                */
                unique_lock( const unique_lock & ) = delete;
                const unique_lock & operator = ( const unique_lock & ) = delete;


                /** Moving constructor/assignment

                @throw nothing
                */
                template < size_t SpinCount >
                unique_lock( unique_lock< SpinCount > && origin ) _NOEXCEPT
                {
                    swap( origin );
                }

                template < size_t SpinCount >
                unique_lock & operator = ( unique_lock< SpinCount > && rhs ) _NOEXCEPT
                {
                    mutex_ = std::nullptr;
                    swap( rhs );
                    return *this;
                }


                /** Swaps with another lock

                @param [in/out] other - lock to be swapped
                @throw nothing
                */
                template< size_t SpinCount >
                void swap( unique_lock< SpinCount > & other ) _NOEXCEPT
                {
                    std::swap( mutex_, other.mutex_ );
                }


                /** Destructor, release EXCLUSIVE lock on related mutex

                @throw nothing
                */
                virtual ~unique_lock() _NOEXCEPT
                {
                    if ( mutex_ ) mutex_->unlock();
                }
            };


            /** Scoped upgrade from SHARED to EXCLUSIVE lock
            */
            template < size_t SpinCount = 1024 >
            class upgrade_lock
            {
                //
                // pointer to related mutex
                //
                rare_write_frequent_read_mutex * mutex_ = std::nullptr;

            public:

                /** Default constructor

                @throw nothing
                */
                upgrade_lock() _NOEXCEPT {}


                /** Locking constructor, upgrades given SHARED lock to EXCLUSIVE

                @param [in] shared_lock - SHARED lock to be upgraded to EXCLUSIVE
                @throw nothing
                */
                template< size_t SpinCount >
                upgrade_lock( const shared_lock< SpinCount > & shared_lock ) _NOEXCEPT : mutex_( shared_lock.mutex_ )
                {
                    if ( mutex_ ) mutex_->lock_upgrade< SpinCount >();
                }


                /** Class is NOT copyable
                */
                upgrade_lock( const upgrade_lock & ) = delete;
                const upgrade_lock & operator = ( const upgrade_lock & ) = delete;


                /* Move constructor/assignment

                @throw nothing
                */
                template < size_t SpinCount >
                upgrade_lock( upgrade_lock< SpinCount > && origin ) _NOEXCEPT
                {
                    swap( origin );
                }

                template < size_t SpinCount >
                upgrade_lock & operator = ( upgrade_lock< SpinCount > && rhs ) _NOEXCEPT
                {
                    mutex_ = std::nullptr;
                    swap( rhs );
                    return *this;
                }


                /** Swaps this lock object with another one

                @param [in/out] other - lock to be swapped
                */
                template< size_t SpinCount >
                void swap( upgrade_lock< SpinCount > & other ) _NOEXCEPT
                {
                    std::swap( mutex_, other.mutex_ );
                }


                /** Destructor, release EXCLUSIVE lock on related mutex

                @throw nothing
                */
                virtual ~upgrade_lock() _NOEXCEPT
                {
                    if ( mutex_ ) mutex_->unlock();
                }
            };
        };
    }
}


#endif
