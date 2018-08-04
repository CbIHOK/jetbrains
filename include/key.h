#ifndef __JB__KEY__H__
#define __JB__KEY__H__


#include <filesystem>


namespace jb
{
    namespace details
    {
        template < typename Policies >
        class Key
        {
            using KeyCharT  = typename Policies::KeyCharT;
            using KeyValueT = typename Policies::KeyPolicy::KeyValueT;
            using KeyRefT   = typename Policies::KeyPolicy::KeyRefT;

            static constexpr KeyCharT Separator = Policies::KeyPolicy::Separator;

            std::filesystem::path key_;
            bool                  valid_;


        public:
            Key(KeyRefT key) : key_(key) {}

            Key() noexcept = default;
            Key(Key &&) noexcept = default;

            Key(const Key &) = delete;
            Key & operator == (const Key &) = delete;

            operator bool() const noexcept
            {
                try
                {
                    return !key_.has_root_name() && key_.has_root_directory() && key_.has_filename();
                }
                catch(...)
                {
                }
                return false;
            }

            friend bool operator == (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) == 0;
            }
            friend bool operator != (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) != 0;
            }
            friend bool operator > (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) > 0;
            }
            friend bool operator >= (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) >= 0;
            }
            friend bool operator < (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) < 0;
            }
            friend bool operator <= (const Key & l, const Key & r) noexcept
            {
                return l.key_.compare(r.key) <= 0;
            }

            auto divorce() const noexcept
            {
                try
                {
                    return std::tuple(key_.parent_path(), key_.filename());
                }
                catch (...)
                {
                }
                return std::tuple(std::filesystem::path(), std::filesystem::path());
            }

            auto split() const noexcept
            {
            };

            auto hash() const noexcept
            {

            }
        };
    }
}


#endif
