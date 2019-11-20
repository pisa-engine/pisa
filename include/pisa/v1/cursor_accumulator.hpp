#pragma once

#include <cstdint>

namespace pisa::v1::accumulate {

struct Add {
    template <typename Score, typename Cursor>
    auto operator()(Score&& score, Cursor&& cursor, std::size_t /* term_idx */)
    {
        score += cursor.payload();
        return score;
    }
};

} // namespace pisa::v1::accumulate
