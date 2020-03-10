#pragma once
#include <type_traits>
namespace pisa {
#ifdef _MSC_VER

    #pragma optimize("", off)
inline void do_not_optimize_dependency_sink(const void*) {}
    #pragma optimize("", on)

template <class T>
void do_not_optimize_away(const T& datum)
{
    doNotOptimizeDependencySink(&datum);
}

#else

namespace detail {
    template <typename T>
    struct do_not_optimize_away_needs_indirect {
        using Decayed = typename std::decay<T>::type;
        constexpr static bool value = !std::is_trivially_copyable<Decayed>::value
            || sizeof(Decayed) > sizeof(long) || std::is_pointer<Decayed>::value;
    };
}  // namespace detail

template <typename T>
auto do_not_optimize_away(const T& datum) ->
    typename std::enable_if<!detail::do_not_optimize_away_needs_indirect<T>::value>::type
{
    asm volatile("" ::"r"(datum));
}

template <typename T>
auto do_not_optimize_away(const T& datum) ->
    typename std::enable_if<detail::do_not_optimize_away_needs_indirect<T>::value>::type
{
    asm volatile("" ::"m"(datum) : "memory");
}
#endif
}  // namespace pisa