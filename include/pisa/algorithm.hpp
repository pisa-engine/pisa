#pragma once

#include <algorithm>

#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    #include <execution>
#endif

namespace pisa {
namespace execution {

    class sequenced_policy {
    };
    class parallel_policy {
    };
    class parallel_unsequenced_policy {
    };

    inline constexpr sequenced_policy seq{};
    inline constexpr parallel_policy par{};
    inline constexpr parallel_unsequenced_policy par_unseq{};

#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)

    [[nodiscard]] constexpr auto to_std(pisa::execution::sequenced_policy /* policy */)
        -> std::execution::sequenced_policy
    {
        return std::execution::seq;
    }

    [[nodiscard]] constexpr auto to_std(pisa::execution::parallel_policy /* policy */)
        -> std::execution::parallel_policy
    {
        return std::execution::par;
    }

    [[nodiscard]] constexpr auto to_std(pisa::execution::parallel_unsequenced_policy /* policy */)
        -> std::execution::parallel_unsequenced_policy
    {
        return std::execution::par_unseq;
    }

#endif

}  // namespace execution

/// A wrapper over `std::transform` that falls back to sequential algorithm if parallel
/// execution is not supported.
template <class ExecutionPolicy, class ForwardIt1, class OutputIt, class UnaryOperation>
OutputIt transform(
    [[maybe_unused]] ExecutionPolicy&& policy,
    ForwardIt1 first,
    ForwardIt1 last,
    OutputIt d_first,
    UnaryOperation unary_op)
{
#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    auto std_policy = pisa::execution::to_std(policy);
    return std::transform(std_policy, first, last, d_first, unary_op);
#else
    return std::transform(first, last, d_first, unary_op);
#endif
}

/// A wrapper over `std::transform` that falls back to sequential algorithm if parallel
/// execution is not supported.
template <class ExecutionPolicy, class ForwardIt1, class ForwardIt2, class OutputIt, class BinaryOperation>
OutputIt transform(
    [[maybe_unused]] ExecutionPolicy&& policy,
    ForwardIt1 first1,
    ForwardIt1 last1,
    ForwardIt2 first2,
    OutputIt d_first,
    BinaryOperation binary_op)
{
#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    auto std_policy = pisa::execution::to_std(policy);
    return std::transform(std_policy, first1, last1, first2, d_first, binary_op);
#else
    return std::transform(first1, last1, first2, d_first, binary_op);
#endif
}

template <class ExecutionPolicy, class RandomIt>
void sort([[maybe_unused]] ExecutionPolicy&& policy, RandomIt first, RandomIt last)
{
#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    auto std_policy = pisa::execution::to_std(policy);
    return std::sort(std_policy, first, last);
#else
    return std::sort(first, last);
#endif
}

template <class ExecutionPolicy, class RandomIt, class Compare>
void sort([[maybe_unused]] ExecutionPolicy&& policy, RandomIt first, RandomIt last, Compare comp)
{
#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    auto std_policy = pisa::execution::to_std(policy);
    return std::sort(std_policy, first, last, comp);
#else
    return std::sort(first, last, comp);
#endif
}

template <class ExecutionPolicy, class ForwardIt, class UnaryFunction2>
void for_each(
    [[maybe_unused]] ExecutionPolicy&& policy, ForwardIt first, ForwardIt last, UnaryFunction2 f)
{
#if defined(_LIBCPP_HAS_PARALLEL_ALGORITHMS)
    auto std_policy = pisa::execution::to_std(policy);
    std::for_each(std_policy, first, last, f);
#else
    std::for_each(first, last, f);
#endif
}

}  // namespace pisa
