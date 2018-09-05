#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__


#include <memory>
#include <tuple>
#include <assert.h>


namespace jb
{
    /** Implements mounting point

    The class realizes 2 responsibillity 

    1) it holds lock over the physical keys for both source and destination of mounting and thus
       prevents removing of them

    2) holds entry point to mounted physical volume, thus making navigation faster

    @tparam Policies - global setting
    */
    template < typename Policies >
    class Storage< Policies >::MountPointImpl
    {
        //
        // short aliases
        //
        using RetCode = typename Storage::RetCode;
        using Key = typename Storage::Key;
        using Value = typename Storage::Value;
        using VirtualVolumeImpl = typename Storage::VirtualVolumeImpl;
        using PhysicalVolumeImpl = typename Storage::PhysicalVolumeImpl;
        using PhysicalVolumeImplP = std::shared_ptr< PhysicalVolumeImpl >;
        using NodeUid = typename PhysicalVolumeImpl::BTree::NodeUid;

        static constexpr auto RootNodeUid = PhysicalVolumeImpl::RootNodeUid;
        static constexpr auto InvalidNodeUid = PhysicalVolumeImpl::InvalidNodeUid;


    public:

        using PathLock = typename PhysicalVolumeImpl::PathLocker::PathLock;
        using execution_connector = typename PhysicalVolumeImpl::execution_connector;


    private:

        //
        // data members
        //
        std::shared_ptr< PhysicalVolumeImpl > physical_volume_;
        NodeUid entry_node_uid_;
        size_t entry_node_level_;
        PathLock locks_;
        RetCode status_;


    public:

        /** The class is not default/copy/move constructible
        */
        MountPointImpl( ) = delete;
        MountPointImpl( MountPointImpl &&) = delete;


        /** Explicit constructor

        @param [in] physical_volume - physical volume to be mounted
        @param [in] physical_path - physical path to be mounted
        @param [in] mount_dst_lock - lock over a destination of mount point
        @throw nothing
        */
        explicit MountPointImpl( PhysicalVolumeImplP physical_volume, const Key & physical_path, PathLock && mount_dst_lock ) noexcept
            : physical_volume_{ physical_volume }
            , locks_{ std::move( mount_dst_lock ) }
        {
            // request physical volume for lock physical of path and entry point UID
            execution_connector in{}; in.second = true;
            execution_connector out{};
            auto [ rc, entry_node_uid, entry_node_level, mount_src_lock ] = physical_volume_->lock_path( RootNodeUid, 0, physical_path, in, out );
            
            status_ = rc;
            entry_node_uid_ = entry_node_uid;
            entry_node_level_ = entry_node_level;
            locks_ << mount_src_lock;
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
        std::tuple < RetCode, NodeUid, size_t, PathLock > lock_path( const Key & relative_path, const execution_connector & in, execution_connector & out ) noexcept
        {
            return physical_volume_->lock_path( entry_node_uid_, entry_node_level_, relative_path, in, out );
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
            auto [rc] = physical_volume_->insert( entry_node_uid_, entry_node_level_, relative_path, subkey, std::move( value ), good_before, overwrite, in, out );
            return rc;
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
            return physical_volume_->get( entry_node_uid_, entry_node_level_, relative_path, in, out );
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
            auto [rc] = physical_volume_->erase( entry_node_uid_, entry_node_level_, relative_path, in, out );
            return rc;
        }
    };
}

#endif
