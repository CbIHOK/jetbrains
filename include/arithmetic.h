#ifndef __JB__ARITHMETIC__H__
#define __JB__ARITHMETIC__H__

namespace arithmetic
{
    constexpr bool is_power_of_2(size_t number) noexcept
    {
        return number && (number & (number - 1)) == 0;
    }
}

#endif
