#ifndef __JB__VIRTUAL_VOLUME__H__
#define __JB__VIRTUAL_VOLUME__H__


#include <memory>


namespace jb
{
    template < typename Policies >
    class Storage< Policies >::VirtualVolume
    {

        template< typename T > friend class Storage;

        class Impl;
        
        using ValueT = typename Policies::ValueT;
        using KeyRefT = typename Policies::KeyPolicy::KeyRefT;


        /** Non-owning reference to implementation
        */
        std::weak_ptr< Impl > impl_;


        /** Instantiating constructor

        Assignes the instance with implementation

        @param [in] impl - implementation instance to be referred
        @throw nothing
        */
        VirtualVolume(const std::shared_ptr< Impl > & impl) noexcept : impl_(impl) {}


    public:

        /** Default constructor

        Creates dummy instance that does not represent an existing virtual volume

        @throw nothing
        */
        VirtualVolume() noexcept = default;


        /** Copy constructor

        Creates new instance as a copy of given one

        @param [in] src - instance to be copied
        @throw nothing
        */
        VirtualVolume(const VirtualVolume & src) = default;


        /** Moving constructor

        Creates new instance by moving from given one

        @param [in] src - instance to move from
        @throw nothing
        */
        VirtualVolume & operator = (const VirtualVolume & src) noexcept = default;


        /** Moving operator

        @param [in] src - instance to be copied
        @return lvalue of the instance
        @throw nothing
        */
        VirtualVolume & operator = (VirtualVolume && src) noexcept = default;


        /** Checks if an instance is valid i.e. it represents existing volume

        @return true if instance is valid
        @throw nothing
        */
        operator bool() const noexcept { return impl_.lock(); }

        friend auto operator == (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.load() == r.load(); }
        friend auto operator != (const VirtualVolume & l, const VirtualVolume & r) noexcept { return l.load() != r.load(); }
    };
}

#include "virtual_volume_impl.h"

#endif