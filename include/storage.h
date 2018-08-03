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

#include "virtual_volume.h"
#include "physical_volume.h"


namespace jb
{
    template < typename Policies >
    class Storage
    {

    public:

        enum class RetCode
        {
            Ok,
            UnknownError,
            InsufficientMemory,
            InvalidVolumeHandle,
            UnknownVolumeHandle
        };

        using VirtualVolume = VirtualVolume< Policies >;

    private:

        //
        // Provides related singletons
        //
        template < typename VolumeT > 
        static auto singletons() noexcept
        {
            using ImplT = typename VolumeT::Impl;

            // c++x guaranties thread safe initialization of the static variables
            static std::mutex guard;

            // definitely opening/closing operations ain't time critical, thus I don't care about
            // structure choice. Also taking into account uniqueness of kept values makes 
            // unordered_set<> quite enough to me. If we introduced some limitation on volume
            // numbers, we would easily adopt unordered_set<> to use cache friendly static allocator
            static std::unordered_set< std::shared_ptr< ImplT > > holder;

            return std::forward_as_tuple(guard, holder);
        }


        //
        // Just a helper
        //
        template < typename VolumeT >
        static auto open() noexcept
        {
            using ImplT = typename VolumeT::Impl;

            try
            {
                auto impl = std::make_shared< ImplT >();
                auto[guard, collection] = implementations< VolumeT >();
                {
                    std::scoped_lock l(guard);
                    if (auto i = collection.insert(impl); i.second)
                    {
                        return std::tuple(RetCode::Ok, VolumeT(impl));
                    }
                }
            }
            catch (const std::bad_alloc &)
            {
                return std::tuple(RetCode::InsufficientMemory, VolumeT());
            }
            catch (const std::exception &)
            {
            }

            return std::tuple(RetCode::UnknownError, VolumeT());
        }


    public:

        /** Creates new physical volumes

        @param [in] path - path to a file representing physical starage
        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of physical storage, possible return codes are
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
        static auto OpenVirtualVolume() noexcept
        {
            return open< VirtualVolume >();
        }


        /** Creates new physical volumes

        @param [in] path - path to a file representing physical starage
        @return std::tuple< ret_code, handle >, if operation succedded, handle keeps valid handle
        of physical storage, possible return codes are
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
        static auto OpenPhysicalVolume(std::filesystem::path && path) noexcept
        {
            return std::make_tuple< RetCode::Ok >;
        }


        /** Closes given volume

        Invalidates given volume handler, destroys associated virtual volume object, and release
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
        @todo consider variadic form
        */
        template< typename T >
        static auto Close(T && volume) noexcept
        {
            using ValidVolumeTypes = boost::mpl::vector< VirtualVolume >;
            using VolumeT = std::decay< T >::type;

            static_assert(boost::mpl::contains< ValidVolumeTypes, VolumeT >::type::value, "Invalid volume type");
            
            try
            {
                auto[guard, collection] = implementations< VolumeT >();
                {
                    std::scoped_lock lock(guard);

                    auto impl = volume.impl_.lock();

                    if ( ! impl )
                    {
                        return std::make_tuple(RetCode::InvalidVolumeHandle);
                    }
                    else if ( auto i = collection.find(impl); i != collection.end() )
                    {
                        collection.erase(i);
                        return std::make_tuple(RetCode::Ok);
                    }
                    else
                    {
                        return std::make_tuple(RetCode::InvalidVolumeHandle);
                    }
                }
            }
            catch (const std::exception &)
            {
            }

            return std::make_tuple(RetCode::UnknownError);
        }


        /** Closes all opened volumes

        Invalidates all opened volume handlers, destroys all virtual volume objects, and release
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
                std::apply( [] (auto handles_of_type) {

                    auto[guard, collection] = handles_of_type;

                    {
                        std::scoped_lock lock(guard);
                        collection.clear();
                    }

                }, std::forward_as_tuple(implementations<VirtualVolume>()));
                
                return std::make_tuple(RetCode::Ok);
            }
            catch (std::exception &)
            {
            }

            return std::make_tuple(RetCode::UnknownError);
        }
    };
}


#endif
