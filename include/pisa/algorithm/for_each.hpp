#pragma once

#include <utility>

namespace pisa {

template <typename Pred>
struct While_Predicate {
    Pred pred;
    template <typename ...Arg>
    [[nodiscard]] bool operator()(Arg const &...arg) const
    {
        return pred(arg...);
    }
};
template <typename Pred>
auto while_holds(Pred pred)
{
    return While_Predicate<Pred>{std::forward<Pred>(pred)};
}

template <typename Iterator, typename Fn, typename Pred>
auto for_each(Iterator first, Iterator last, Fn fn, While_Predicate<Pred> pred)
{
    for (; first != last; ++first) {
        fn(*first);
        if (not pred(*first)) {
            break;
        }
    }
    return first;
}

template <typename Iterator, typename Pred, typename Fn>
auto for_each(Iterator first, Iterator last, While_Predicate<Pred> pred, Fn fn)
{
    for (; first != last; ++first) {
        if (not pred(*first)) {
            break;
        }
        fn(*first);
    }
    return first;
}

template <typename Iterator1, typename Iterator2, typename Pred, typename Function>
std::pair<Iterator1, Iterator2> for_each_pair(Iterator1 first1,
                                              Iterator1 last1,
                                              Iterator2 first2,
                                              Iterator2 last2,
                                              While_Predicate<Pred> pred,
                                              Function fn)
{
    for (; first1 != last1; ++first1, ++first2) {
        if (not stop_condition(*first1, *first2)) {
            break;
        }
        fn(*first1, *first2);
    }
    return std::pair<Iterator1, Iterator2>(first1, first2);
}

} // namespace pisa
