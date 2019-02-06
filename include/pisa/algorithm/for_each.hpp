#pragma once

namespace pisa {

template <typename Iterator, typename StopCond, typename Function>
Iterator for_each_until(Iterator first, Iterator last, StopCond stop_condition, Function fn)
{
    for (; first != last; ++first) {
        if (stop_condition(*first)) {
            break;
        }
        fn(*first);
    }
    return first;
}

} // namespace pisa
