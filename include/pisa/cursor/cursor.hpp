#pragma once

#include <vector>

#include "query.hpp"
#include "util/compiler_attribute.hpp"

namespace pisa {

template <typename InputIterator, typename OutputIterator, typename BinaryOperation, typename Tp>
OutputIterator inclusive_scan(
    InputIterator first, InputIterator last, OutputIterator result, BinaryOperation binary_op, Tp init)
{
    for (; first != last; ++first) {
        *result++ = init = binary_op(init, *first);
    }
    return result;
}

struct Add {
    template <typename Score, typename Cursor>
    PISA_ALWAYSINLINE auto operator()(Score&& score, Cursor&& cursor)
    {
        score += cursor.score();
        return score;
    }
};

template <typename T>
PISA_ALWAYSINLINE void init_payload(T& payload, T const& initial_value)
{
    payload = initial_value;
}

template <>
PISA_ALWAYSINLINE void
init_payload(std::vector<float>& payload, std::vector<float> const& initial_value)
{
    std::copy(initial_value.begin(), initial_value.end(), payload.begin());
}

template <typename Index>
[[nodiscard]] auto make_cursors(Index const& index, QueryRequest query)
{
    auto term_ids = query.term_ids();
    std::vector<typename Index::document_enumerator> cursors;
    cursors.reserve(term_ids.size());
    std::transform(term_ids.begin(), term_ids.end(), std::back_inserter(cursors), [&](auto&& term_id) {
        return index[term_id];
    });

    return cursors;
}

template <typename Cursor, typename Payload, typename AccumulateFn>
class CursorJoin {
  public:
    using value_type = std::uint32_t;
    using payload_type = Payload;
    using cursor_type = Cursor;

    CursorJoin(payload_type init, AccumulateFn accumulate) : m_init(init), m_accumulate(accumulate)
    {
        init_payload();
    }

    [[nodiscard]] PISA_ALWAYSINLINE auto docid() const noexcept -> value_type
    {
        return m_current_value;
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto payload() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto score() const noexcept -> Payload const&
    {
        return m_current_payload;
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto sentinel() const noexcept -> std::uint32_t
    {
        return m_sentinel;
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto universe() const noexcept -> std::uint32_t
    {
        return m_sentinel;
    }
    [[nodiscard]] PISA_ALWAYSINLINE auto empty() const noexcept -> bool
    {
        return m_current_value >= sentinel();
    }

  protected:
    PISA_ALWAYSINLINE void set_sentinel(value_type sentinel) { m_sentinel = sentinel; }
    PISA_ALWAYSINLINE void set_current_value(value_type docid) { m_current_value = docid; }
    PISA_ALWAYSINLINE void set_current_payload(value_type payload) { m_current_payload = payload; }
    PISA_ALWAYSINLINE void init_payload() { ::pisa::init_payload(m_current_payload, m_init); }
    PISA_ALWAYSINLINE void accumulate(Cursor& cursor)
    {
        m_current_payload = m_accumulate(m_current_payload, cursor);
    }

  private:
    payload_type m_init;
    AccumulateFn m_accumulate;

    value_type m_sentinel{};
    value_type m_current_value{};
    payload_type m_current_payload{};
};

}  // namespace pisa
