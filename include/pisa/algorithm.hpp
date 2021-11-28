#pragma once

#include <algorithm>

#if __has_include(<execution>)
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

#if __has_include(<execution>)

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
#if __has_include(<execution>)
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
    ExecutionPolicy&& policy,
    ForwardIt1 first1,
    ForwardIt1 last1,
    ForwardIt2 first2,
    OutputIt d_first,
    BinaryOperation binary_op)
{
#if __has_include(<execution>)
    auto std_policy = pisa::execution::to_std(policy);
    return std::transform(std_policy, first1, last1, first2, d_first, binary_op);
#else
    return std::transform(first1, last1, first2, d_first, unary_op);
#endif
}

}  // namespace pisa
