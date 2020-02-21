#pragma once

#include <algorithm>
#include <numeric>

#include <gsl/span>
#include <range/v3/algorithm/find_if_not.hpp>
#include <range/v3/algorithm/sort.hpp>

namespace pisa::v1 {

namespace func {

    /// Calls `max_score()` method on any passed object. The default projection for
    /// `maxscore_partition`.
    struct max_score {
        template <typename Cursor>
        auto operator()(Cursor&& cursor) -> float
        {
            return cursor.max_score();
        }
    };

} // namespace func

/// Partitions a list of cursors into essential and non-essential parts as in MaxScore algorithm
/// first proposed by [Turtle and
/// Flood](https://www.sciencedirect.com/science/article/pii/030645739500020H).
///
/// # Details
///
/// This function takes a span of (max-score) cursors that participate in a query, and the current
/// threshold. By default, it retrieves the max scores from cursors by calling `max_score()` method.
/// However, a different can be used by passing a projection. For example, if instead of
/// partitioning actual posting list cursors, you want to partition a vector of pairs `(term,
/// max-score)`, then you may pass
/// `[](auto&& c) { return c.second; }` as the projection.
///
/// # Complexity
///
/// Note that this function **will** sort the cursors by their max scores to ensure correct
/// partitioning, and therefore it may not be suitable to update an existing partition.
template <typename C, typename P = func::max_score>
auto maxscore_partition(gsl::span<C> cursors, float threshold, P projection = func::max_score{})
    -> std::pair<gsl::span<C>, gsl::span<C>>
{
    ranges::sort(cursors.begin(), cursors.end(), std::less{}, projection);
    float bound = 0;
    auto mid = ranges::find_if_not(cursors, [&](auto&& cursor) {
        bound += projection(cursor);
        return bound < threshold;
    });
    auto non_essential_count = std::distance(cursors.begin(), mid);
    return std::make_pair(cursors.first(non_essential_count), cursors.subspan(non_essential_count));
}

/// This cursor operator takes a number of essential cursors (in an arbitrary order)
/// and a list of lookup cursors. The documents traversed will be in the DaaT order,
/// and the following documents will be skipped:
///  - documents that do not appear in any of the essential cursors,
///  - documents that at the moment of their traversal are irrelevant (see below).
///
/// # Threshold
///
/// This operator takes a callable object that returns `true` only if a given score
/// has a chance to be in the final result set. It is used to decide whether or not
/// to perform further lookups for the given document. The score passed to the function
/// is such that when it returns `false`, we know that it will return `false` for the
/// rest of the lookup cursors, and therefore we can skip that document.
/// Note that such document will never be returned by this cursor. Instead, we will
/// proceed to the next document to see if it can land in the final result set, and so on.
///
/// # Accumulating Scores
///
/// Another parameter taken by this operator is a callable that accumulates payloads
/// for one document ID. The function is very similar to what you would pass to
/// `std::accumulate`: it takes the accumulator (either by reference or value),
/// and a reference to the cursor. It must return an updated accumulator.
/// For example, a simple accumulator that simply sums all payloads for each document,
/// can be: `[](float score, auto&& cursor) { return score + cursor.payload(); }`.
/// Note that you can accumulate "heavier" objects by taking and returning a reference:
/// ```
/// [](auto& acc, auto&& cursor) {
///     // Do something with acc
///     return acc;
/// }
/// ```
/// Before the first call to the accumulating function, the accumulated payload will be
/// initialized to the value `init` passed in the constructor. This will also be the
/// type of the payload returned by this cursor.
///
/// # Passing Cursors
///
/// Both essential and lookup cursors are passed by value and moved into a member.
/// It is thus important to pass either a temporary, a view, or a moved object to the constructor.
/// It is recommended to pass the ownership through an rvalue, as the cursors will be consumed
/// either way. However, in rare cases when the cursors need to be read after use
/// (for example to get their size or max score) or if essential and lookup cursors are in one
/// container and you want to avoid moving them, you may pass a view such as `gsl::span`.
/// However, it is discouraged in general case due to potential lifetime issues and dangling
/// references.
template <typename EssentialCursors,
          typename LookupCursors,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
struct UnionLookupJoin {

    using essential_cursor_type = typename EssentialCursors::value_type;
    using lookup_cursor_type = typename LookupCursors::value_type;

    using payload_type = Payload;
    using value_type = std::decay_t<decltype(*std::declval<essential_cursor_type>())>;

    using essential_iterator_category =
        typename std::iterator_traits<typename EssentialCursors::iterator>::iterator_category;

    static_assert(std::is_base_of<std::random_access_iterator_tag, essential_iterator_category>(),
                  "cursors must be stored in a random access container");

    UnionLookupJoin(EssentialCursors essential_cursors,
                    LookupCursors lookup_cursors,
                    Payload init,
                    AccumulateFn accumulate,
                    ThresholdFn above_threshold,
                    Inspect* inspect = nullptr)
        : m_essential_cursors(std::move(essential_cursors)),
          m_lookup_cursors(std::move(lookup_cursors)),
          m_init(std::move(init)),
          m_accumulate(std::move(accumulate)),
          m_above_threshold(std::move(above_threshold)),
          m_inspect(inspect)
    {
        if (m_essential_cursors.empty()) {
            if (m_lookup_cursors.empty()) {
                m_sentinel = std::numeric_limits<value_type>::max();
            } else {
                m_sentinel = min_sentinel(m_lookup_cursors);
            }
            m_current_value = m_sentinel;
            m_current_payload = m_init;
            return;
        }
        m_lookup_cumulative_upper_bound = std::accumulate(
            m_lookup_cursors.begin(), m_lookup_cursors.end(), 0.0F, [](auto acc, auto&& cursor) {
                return acc + cursor.max_score();
            });
        m_next_docid = min_value(m_essential_cursors);
        m_sentinel = min_sentinel(m_essential_cursors);
        advance();
    }

    [[nodiscard]] constexpr auto operator*() const noexcept -> value_type
    {
        return m_current_value;
    }
    [[nodiscard]] constexpr auto value() const noexcept -> value_type { return m_current_value; }
    [[nodiscard]] constexpr auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] constexpr auto sentinel() const noexcept -> std::uint32_t { return m_sentinel; }

    constexpr void advance()
    {
        bool exit = false;
        while (not exit) {
            if (PISA_UNLIKELY(m_next_docid >= sentinel())) {
                m_current_value = sentinel();
                m_current_payload = m_init;
                return;
            }
            m_current_payload = m_init;
            m_current_value = std::exchange(m_next_docid, sentinel());

            if constexpr (not std::is_void_v<Inspect>) {
                m_inspect->document();
            }

            for (auto&& cursor : m_essential_cursors) {
                if (cursor.value() == m_current_value) {
                    if constexpr (not std::is_void_v<Inspect>) {
                        m_inspect->posting();
                    }
                    m_current_payload = m_accumulate(m_current_payload, cursor);
                    cursor.advance();
                }
                if (auto docid = cursor.value(); docid < m_next_docid) {
                    m_next_docid = docid;
                }
            }

            exit = true;
            auto lookup_bound = m_lookup_cumulative_upper_bound;
            for (auto&& cursor : m_lookup_cursors) {
                //if (m_current_value == 2288) {
                //    std::cout << fmt::format(
                //        "[checking] doc: {}\tscore: {}\tbound: {} (is above = {})\tms = {}\n",
                //        m_current_value,
                //        m_current_payload,
                //        m_current_payload + lookup_bound,
                //        m_above_threshold(m_current_payload + lookup_bound),
                //        cursor.max_score());
                //}
                if (not m_above_threshold(m_current_payload + lookup_bound)) {
                    exit = false;
                    break;
                }
                cursor.advance_to_geq(m_current_value);
                //std::cout << fmt::format(
                //    "doc: {}\tbound: {}\n", m_current_value, m_current_payload + lookup_bound);
                if constexpr (not std::is_void_v<Inspect>) {
                    m_inspect->lookup();
                }
                if (cursor.value() == m_current_value) {
                    m_current_payload = m_accumulate(m_current_payload, cursor);
                }
                lookup_bound -= cursor.max_score();
            }
        }
        m_position += 1;
    }

    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t { return m_position; }
    [[nodiscard]] constexpr auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

   private:
    EssentialCursors m_essential_cursors;
    LookupCursors m_lookup_cursors;
    payload_type m_init;
    AccumulateFn m_accumulate;
    ThresholdFn m_above_threshold;

    value_type m_current_value{};
    value_type m_sentinel{};
    payload_type m_current_payload{};
    std::uint32_t m_next_docid{};
    payload_type m_previous_threshold{};
    payload_type m_lookup_cumulative_upper_bound{};
    std::size_t m_position = 0;

    Inspect* m_inspect;
};

/// Convenience function to construct a `UnionLookupJoin` cursor operator.
/// See the struct documentation for more information.
template <typename EssentialCursors,
          typename LookupCursors,
          typename Payload,
          typename AccumulateFn,
          typename ThresholdFn,
          typename Inspect = void>
auto join_union_lookup(EssentialCursors essential_cursors,
                       LookupCursors lookup_cursors,
                       Payload init,
                       AccumulateFn accumulate,
                       ThresholdFn threshold,
                       Inspect* inspect = nullptr)
{
    return UnionLookupJoin<EssentialCursors,
                           LookupCursors,
                           Payload,
                           AccumulateFn,
                           ThresholdFn,
                           Inspect>(std::move(essential_cursors),
                                    std::move(lookup_cursors),
                                    std::move(init),
                                    std::move(accumulate),
                                    std::move(threshold),
                                    inspect);
}

} // namespace pisa::v1
