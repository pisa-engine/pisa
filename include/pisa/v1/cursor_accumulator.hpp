#pragma once

#include <cstdint>
#include <type_traits>

namespace pisa::v1::accumulators {

struct Add {
    template <typename Score, typename Cursor>
    auto operator()(Score&& score, Cursor&& cursor)
    {
        score += cursor.payload();
        return score;
    }
};

template <typename Inspect = void>
struct InspectAdd {
    constexpr explicit InspectAdd(Inspect* inspect) : m_inspect(inspect) {}

    template <typename Score, typename Cursor>
    auto operator()(Score&& score, Cursor&& cursor, std::size_t /* term_idx */)
    {
        if constexpr (not std::is_void_v<Inspect>) {
            m_inspect->posting();
        }
        score += cursor.payload();
        return score;
    }

   private:
    Inspect* m_inspect;
};

} // namespace pisa::v1::accumulate
