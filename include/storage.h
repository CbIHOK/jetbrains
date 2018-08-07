#ifndef __JB__STORAGE__H__
#define __JB__STORAGE__H__


#include <memory>
#include <mutex>
#include <unordered_set>
#include <tuple>
#include <type_traits>
#include <filesystem>

#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>


namespace jb
{
    struct DefaultPad {};

    template <typename Policies, typename Pad, typename T> struct hash {};

    template < typename Policies, typename Pad = DefaultPad >
    class Storage
    {
        friend typename Pad;
        friend class VirtualVolume;

        /** Helper, provides related singletons
        */
        template < typename VolumeT > 
        static auto singletons() noexcept
        {
            using ImplT = typename VolumeT::Impl;

            // c++x guaranties thread safe initialization of the static variables
            static std::mutex guard;

            // definitely opening/closing operations ain't time critical, thus I don't care about
            // structure choice. Also uniqueness of kept values makes unordered_set<> quite enough
            // to me
            static std::unordered_set< std::shared_ptr< ImplT > > holder;

            return std::forward_as_tuple(guard, holder);
        }


        //
        // Just a helper
        //
        template < typename VolumeT, typename ... Args >
        static auto open(Args&& ... args) noexcept
        {
            using ImplT = typename VolumeT::Impl;

            // noexcept guarantee unlike tuple
            std::pair< RetCode, VolumeT > result{ RetCode::UnknownError, std::move(VolumeT()) };

            try
            {
                auto impl = std::make_shared< ImplT >( std::forward(args)... );
                auto[guard, collection] = singletons< VolumeT >();
                {
                    std::scoped_lock l(guard);
                    if (auto i = collection.insert(impl); i.second)
                    {
                        result.first = RetCode::Ok;
                        result.second = std::move(VolumeT(impl));
                    }
                }
            }
            catch (const std::bad_alloc &)
            {
                result.first = RetCode::InsufficientMemory;
            }
            catch (...)
            {
            }

            return result;
        }


        /* Helper, closes given handler

        Invalidates given volume handler, destroys associated Virtual Volume object, and release
        allocated resources. If there are operations locking the volume, the function postpones
        the actions until all locking operations get completed. Unlocked volume is destroyed
        immediately.

        @tparam T - volume type, auto deducing implied
        @param [in] volume - volume to be closed

        @return std::tuple< ret_code >
        ret_code == RetCode::Ok => operation succedded
        ret_code == RetCode::InvalidVolumeHandle => passed handle does not refer a volume
        ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        */
        template< typename T >
        static auto close(T * volume) noexcept
        {
            assert(volume);

            using ValidVolumeTypes = boost::mpl::vector< VirtualVolume, PhysicalVolume >;
            using VolumeT = std::decay< T >::type;

            static_assert(boost::mpl::contains< ValidVolumeTypes, VolumeT >::type::value, "Invalid volume type");

            try
            {
                auto[guard, collection] = singletons< VolumeT >();
                {
                    std::scoped_lock lock(guard);

                    auto impl = volume->impl_.lock();

                    if (!impl)
                    {
                        return RetCode::InvalidHandle;
                    }
                    else if (auto i = collection.find(impl); i != collection.end())
                    {
                        collection.erase(i);
                        return RetCode::Ok;
                    }
                    else
                    {
                        return RetCode::InvalidHandle;
                    }
                }
            }
            catch (...)
            {
            }

            return RetCode::UnknownError;
        }


    public:

        /** Enumerates all possible return codes
        */
        enum class RetCode
        {
            Ok,                     ///< Operation succedded
            UnknownError,           ///< Something wrong happened
            InsufficientMemory,     ///< Operation failed due to low memory
            InvalidHandle,          ///< Given handle does not address valid object
            MountPointLimitReached, ///< Virtual Volume already has maximum number of Mounts Points
            AlreadyExists,          ///< Such Mount Point already exist
            InvalidLogicalKey,
            InvalidPhysicalKey,
        };


        /** Virtual volume type
        */
        class VirtualVolume;
        class PhysicalVolume;
        class MountPoint;


        /** Creates new Virtual Volumes

        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of Virtual Volume, possible return codes are
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::InsufficientMemory => operation failed due to low memory
            ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        */
        [[nodiscard]]
        static auto OpenVirtualVolume() noexcept
        {
            return open< VirtualVolume >();
        }


        /** Creates new physical volumes

        @param [in] path - path to a file representing physical starage
        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of physical volume, possible return codes are
           ret_code == RetCode::Ok => operation succedded
           ret_code == RetCode::InsufficientMemory => operation failed due to low memory
           ret_code == RetCode::FileNotFound => given file path leads to nowhere
           ret_code == RetCode::FileAlreadyOpened => given file is already opened by this process
           ret_code == RetCode::FileIsLocked => given file is locked by another process
           ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        @todo implement
        */
        [[nodiscard]]
        static auto OpenPhysicalVolume(std::filesystem::path && path ) noexcept
        {
            return open< PhysicalVolume >(path);
        }


        /** Closes all opened volumes

        Invalidates all opened volume handlers, destroys all Virtual Volume objects, and release
        allocated resources. If there are operations locking a volume, the function postpones
        destruction until all locking operations get completed. Unlocked volumes are destroyed
        immediately.

        @return std::tuple< ret_code >
            ret_code == RetCode::Ok => operation succedded
            ret_code == RetCode::UnknownError => something went really wrong

        @throw nothing
        */
        static auto CloseAll() noexcept
        {
            try
            {
                auto close_all = [] (auto handles_of_type) {

                    auto[guard, collection] = handles_of_type;

                    {
                        std::scoped_lock lock(guard);
                        collection.clear();
                    }

                };
                
                close_all(singletons<VirtualVolume>());
                close_all(singletons<PhysicalVolume>());

                return RetCode::Ok;
            }
            catch (...)
            {
            }

            return RetCode::UnknownError;
        }
    };
}


#include "virtual_volume.h"
#include "physical_volume.h"
#include "mount_point.h"


#endif
