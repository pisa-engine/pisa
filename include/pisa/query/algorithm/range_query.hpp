#pragma once

#include "cursor/block_max_scored_cursor.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "wand_data_range.hpp"

namespace pisa {
using WandTypeRange = wand_data_range<128, 1024, bm25>;

template <typename QueryAlg, size_t block_size = 128>
struct range_query {

    range_query(uint64_t k) : m_k(k), m_topk(k) {}

    template <typename CursorRange>
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid, size_t range_size)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }

        for (size_t end = range_size; end + range_size <= max_docid; end += range_size) {
            process_range(cursors, end, range_size);
        }
        process_range(cursors, max_docid, range_size);

        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    template <typename WandType, typename CursorType>
    auto get_enums(CursorType &&cursors)
    {
        using wdata_enum = typename WandType::wand_data_enumerator;
        std::vector<wdata_enum> enums;
        for (auto &c : cursors) {
            enums.push_back(c.w);
        }
        return enums;
    }

    template <typename CursorRange>
    void process_range(CursorRange &&cursors, size_t end, size_t range_size)
    {
        size_t begin = end - range_size;
        auto enums = get_enums<wand_data<bm25, WandTypeRange>, CursorRange>(cursors);
        //auto enums = get_enums<WandTypeRange, CursorRange>(cursors);
        auto live_dead = WandTypeRange::compute_live_blocks(
            enums, m_topk.getThreshold(), std::make_pair(begin, end));
        for (int block = 0; block < live_dead.size(); ++block) {
            if (live_dead[block]) {
                size_t block_end = begin + (block + 1) * block_size;
                for (auto &cursor : cursors) {
                    cursor.docs_enum.next_geq(block_end - block_size);
                }
                QueryAlg query_alg(m_k);
                query_alg(cursors, end);
                auto small_topk = query_alg.topk();
                for (const auto &entry : small_topk) {
                    m_topk.insert(entry.first, entry.second);
                }
            }
        }
    }

   private:
    uint64_t m_k;
    topk_queue m_topk;
};

} // namespace pisa
