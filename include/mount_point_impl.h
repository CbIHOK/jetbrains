#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__


#include <memory>
#include <tuple>
#include <assert.h>


namespace jb
{
    /** Implements mounting point

    @tparam Policies - global setting
    @tparam Pad - test stuff
    */
    template < typename Policies >
    class Storage< Policies >::MountPointImpl
    {

    public:

        //
        // short aliases
        //
        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using KeyValue = typename Key::ValueT;
        using Value = typename Storage::Value;
        using VirtualVolumeImpl = typename Storage::VirtualVolumeImpl;
        using PhysicalVolumeImpl = typename Storage::PhysicalVolumeImpl;
        using PhysicalVolumeImplP = std::shared_ptr< PhysicalVolumeImpl >;
        using PathLock = typename PhysicalVolumeImpl::PathLocker::PathLock;
        using NodeUid = typename PhysicalVolumeImpl::BTree::NodeUid;
        using execution_connector = typename PhysicalVolumeImpl::execution_connector;


    private:

        std::shared_ptr< PhysicalVolumeImpl > physical_volume_;
        NodeUid entry_node_uid_;
        KeyValue entry_path_;
        PathLock locks_;
        RetCode status_;
        std::shared_lock< std::shared_mutex > volume_locker_;


    public:

        /** The class is not default/copy/move constructible
        */
        MountPointImpl( ) = delete;
        MountPointImpl( MountPointImpl &&) = delete;


        /** Explicit constructor

        @param [in] physical_volume - physical volume to be connected
        @param [in] physical_path - physical path to be connected
        @param [in] mounted_to_lock - lock object that prevents removing a physical node in whose space we're mounting
        */
        explicit MountPointImpl( PhysicalVolumeImplP physical_volume, const Key & physical_path, PathLock && mounted_to_lock )
            : physical_volume_{ physical_volume }
            , entry_path_{ ( KeyValue )physical_path }
            , locks_{ std::move( mounted_to_lock ) }
        {
            using namespace std;

            assert( physical_volume_ );
            assert( physical_path.is_path() );

            // request physical volume for lock physical of path and entry point UID
            execution_connector in{}; in.second = true;
            execution_connector out{};
            auto res = physical_volume_->lock_path( 0, Key::root(), physical_path, in, out );
            
            status_ = std::get< RetCode >( res );
            entry_node_uid_ = std::get< NodeUid >( res );
            locks_ << move( std::get< PathLock >( res ) );
        }


        /** Provides mount status

        @retval RetVal - mounting status
        @throw nothing
        */
        auto status() const noexcept { return status_; }


        /** Provides connected physical volume

        @retval std::shared_ptr< PhysicalVolumeImpl > - connected physical volume
        @throw nothing
        */
        auto physical_volume() const noexcept { return physical_volume_; }


        /** Forwards lock_path() request to connected physical volume

        @param [in] relative_path - a relative path to be locked
        @param [in] in - incoming execution events
        @param [out] out - outgoing execution events
        @retval RetCode - operation status
        @retval NodeUid - internal UID of a node by path
        @retval PathLock - lock object that prevents erasing of any node laying on relative path
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple < RetCode, NodeUid, PathLock > lock_path( const Key & relative_path, const execution_connector & in, execution_connector & out ) noexcept
        {
            assert( relative_path );

            try
            {
                return physical_volume_->lock_path( entry_node_uid_, Key{ entry_path_ }, relative_path, in, out );
            }
            catch ( ... )
            {}

            return { RetCode::UnknownError, NodeUid{}, PathLock{} };
        }


        /** Forwards insert() request to connected physical volume

        @param [in] relative_path - a path relative to the mount point
        @param [in] subkey - subkey to be inserted
        @param [in] value - value to be assigned
        @param [in] good_before - expiration time for the subkey
        @param [in] overwrite - overwrite node if exists
        @param [in] in - incoming execution events
        @param [out] out - outgoing execution events
        @retval RetCode - operation status
        @throw nothing
        */
        [[ nodiscard ]]
        RetCode insert( const Key & relative_path, const Key & subkey, Value && value, uint64_t good_before, bool overwrite, const execution_connector & in, execution_connector & out ) noexcept
        {
            assert( relative_path.is_path() );
            assert( subkey.is_leaf() );

            try
            {
                auto [ rc ] = physical_volume_->insert( entry_node_uid_, Key{ entry_path_ }, relative_path, subkey, std::move( value ), good_before, overwrite, in, out );
                return rc;
            }
            catch ( ... )
            {
            }

            return RetCode::UnknownError;
        }


        /** Forwards get() request to connected physical volume

        @param [in] relative_path - a path relative to the mount point
        @param [in] in - incoming execution events
        @param [out] out - outgoing execution events
        @retval RetCode - operation status
        @retval Value - contains requested value if operation succeeded
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode, Value > get( const Key & relative_path, const execution_connector & in, execution_connector & out ) noexcept
        {
            assert( relative_path.is_path() );

            try
            {
                return physical_volume_->get( entry_node_uid_, Key{ entry_path_ }, relative_path, in, out );
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError, Value{} };
        }


        /** Forwards erase() request to connected physical volume

        @param [in] relative_path - a path relative to the mount point
        @param [in] in - incoming execution events
        @param [out] out - outgoing execution events
        @retval RetCode - operation status
        @throw nothing
        */
        [[ nodiscard ]]
        std::tuple< RetCode > erase( const Key & relative_path, const execution_connector & in, execution_connector & out ) noexcept
        {
            assert( relative_path.is_path() );

            try
            {
                return physical_volume_->erase( entry_node_uid_, Key{ entry_path_ }, relative_path, in, out );
            }
            catch ( ... )
            {
            }

            return { RetCode::UnknownError };
        }
    };
}

#endif
