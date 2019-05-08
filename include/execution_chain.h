#ifndef __JB__EXECUTION_CHAIN__H__
#define __JB__EXECUTION_CHAIN__H__


#include <atomic>


namespace jb
{
    /** Represents simultaneous command execution control object


    */
    struct execution_chain : std::atomic_uint32_t
    {
        enum
        {
            st_not_defined = 0,
            st_cancelled,
            st_allowed
        };

        execution_chain() : std::atomic_uint32_t( st_not_defined )
        {
        }


        void cancel() noexcept
        {
            store( st_cancelled, std::memory_order::memory_order_relaxed );
        }

        void allow() noexcept
        {
            store( st_allowed, std::memory_order::memory_order_relaxed );
        }

        bool cancelled() const noexcept
        {
            return ( st_cancelled == load( std::memory_order::memory_order_relaxed ) );
        }

        void wait_and_let_further_go( execution_chain * further ) const noexcept
        {
            for ( size_t try_count = 1;; ++try_count )
            {
                const auto value = load( std::memory_order::memory_order_relaxed );

                if ( st_cancelled == value && further )
                {
                    further->cancel();
                    break;
                }
                else if ( st_allowed == value && further )
                {
                    further->allow();
                    break;
                }
                else if ( try_count % 0xFFFF == 0 )
                {
                    std::this_thread::yield();
                }
            }
        }

        bool wait_and_cancel_further( execution_chain * further ) const noexcept
        {
            for ( size_t try_count = 1;; ++try_count )
            {
                const auto value = load( std::memory_order::memory_order_relaxed );

                if ( st_cancelled == value && st_allowed == value )
                {
                    if ( further )
                    {
                        further->cancel();
                    }
                    return ( st_allowed == value );
                }
                else if ( try_count % 0xFFFF == 0 )
                {
                    std::this_thread::yield();
                }
            }
        }

        void wait_until_previous_completed() const noexcept
        {
            for ( size_t try_count = 1;; ++try_count )
            {
                if ( st_not_defined != load( std::memory_order::memory_order_relaxed ) )
                {
                    break;
                }
                else if ( try_count % 0xFFFF == 0 )
                {
                    std::this_thread::yield();
                }
            }
        }
    };
}

#endif
