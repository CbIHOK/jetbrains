#ifndef __JB__MOUNT_POINT_IMPL__H__
#define __JB__MOUNT_POINT_IMPL__H__


#include <memory>
#include <assert.h>
#include <atomic>


namespace jb
{
    template < typename Policies, typename Pad >
    class Storage< Policies, Pad >::MountPointImpl
    {
        friend typename Pad;

    public:

        using RetCode = ::jb::RetCode;
        using Key = Storage::Key;
        using KeyValue = typename Key::ValueT;
        using Value = typename Storage::Value;
        using Timestamp = typename Storage::Timestamp;
        using VirtualVolumeImpl = typename Storage::VirtualVolumeImpl;
        using PhysicalVolumeImpl = typename Storage::PhysicalVolumeImpl;
        using PhysicalVolumeImplP = std::shared_ptr< PhysicalVolumeImpl >;
        using NodeLock = typename PhysicalVolumeImpl::NodeLock;
        using NodeUid = typename PhysicalVolumeImpl::NodeUid;

        MountPointImpl( ) = delete;
        MountPointImpl( MountPointImpl &&) = delete;

        MountPointImpl( PhysicalVolumeImplP physical_volume, const Key & physical_path, NodeLock && mounted_to_lock )
            : physical_volume_{ physical_volume }
            , entry_path_{ physical_path }
            , locks_{ std::move( mounted_to_lock ) }
        {
            using namespace std;

            assert( physical_volume_ );

            PhysicalVolumeImpl::execution_chain in{ false, true };
            PhysicalVolumeImpl::execution_chain out{};

            tuple< RetCode, NodeUid, NodeLock > res = physical_volume_->lock_path( 0, Key{}, physical_path, in, out );
            
            status_ = std::get< 0 >( res );
            entry_node_uid_ = std::get< 1 >( res );
            locks_ << move( std::get< NodeLock >( res ) );
        }

        auto physical_volume() const noexcept
        {
            return physical_volume_;
        }
        
        auto lock_path( const Key & relative_path, std::atomic_bool & cancel, std::atomic_bool & doit )
        {
            return physical_volume_->lock_path( entry_node_uid_, entry_path_, relative_path, cancel, doit );
        }

        auto insert( const Key & relative_path, const Key & subkey, Value && value, Timestamp && good_before, std::atomic_bool & cancel, std::atomic_bool & doit )
        {
            return physical_volume_->insert( entry_node_uid_, entry_path_, relative_path, subkey, move( value ), move( good_before ), cancel, doit );
        }

        auto get( const Key & relative_path, std::atomic_bool & cancel, std::atomic_bool & doit )
        {
            return physical_volume_->get( entry_node_uid_, entry_path_, relative_path, cancel, doit );
        }

        auto erase( const Key & relative_path, std::atomic_bool & cancel, std::atomic_bool & doit )
        {
            return physical_volume_->erase( entry_node_uid_, entry_path_, relative_path, cancel, doit );
        }

    private:
        std::shared_ptr< PhysicalVolumeImpl > physical_volume_;
        NodeUid entry_node_uid_;
        KeyValue entry_path_;
        NodeLock locks_;
        RetCode status_;
    };
}

#endif
