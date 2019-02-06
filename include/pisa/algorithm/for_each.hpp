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

template <typename Iterator1, typename Iterator2, typename StopCond, typename Function>
std::pair<Iterator1, Iterator2> for_each_pair_until(Iterator1 first1,
                                                    Iterator1 last1,
                                                    Iterator2 first2,
                                                    Iterator2 last2,
                                                    StopCond stop_condition,
                                                    Function fn)
{
    for (; first1 != last1; ++first1, ++first2) {
        if (stop_condition(*first1, *first2)) {
            break;
        }
        fn(*first1, *first2);
    }
    return std::pair<Iterator1, Iterator2>(first1, first2);
}

} // namespace pisa
