#pragma once

#include <chrono>

#include "cursor/block_max_scored_cursor.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "wand_data_range.hpp"

#include <math.h>
namespace pisa {
using WandTypeRange = wand_data_range<128, 1024, bm25>;

template <typename QueryAlg>
struct range_query {

    range_query(uint64_t k) : m_k(k), m_topk(k) {}

    template <typename CursorRange>
    // range_size: multiple of 128
    uint64_t operator()(CursorRange &&cursors, uint64_t max_docid, size_t range_size)
    {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        // std::chrono::nanoseconds elapsed(0);
        // auto start_interval = std::chrono::steady_clock::now();
        for (size_t end = range_size; end + range_size <= max_docid; end += range_size) {
            process_range(cursors, end, range_size);
        }
        process_range(cursors, max_docid, range_size);

        m_topk.finalize();
        // auto end_interval = std::chrono::steady_clock::now();
        // elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_interval - start_interval);
        // long time = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        // std::cout << time << std::endl;
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

    template <typename CursorRange>
    void process_range(CursorRange &&cursors, size_t end, size_t range_size)
    {
        size_t begin = (end / range_size - 1) * range_size;
        auto live_dead = WandTypeRange::compute_live_blocks(
            cursors, m_topk.threshold(), std::make_pair(begin, end));
        for (int block = 0; block < live_dead.size(); ++block) {
            if (live_dead[block]) {
            // if(1) {
                size_t block_end = std::min(begin + (block + 1) * 128, end);
                // std::cout << begin << " " << end << " " << block_end << std::endl;
                for (auto &cursor : cursors) {
                    cursor.docs_enum.next_geq(begin + block * 128);
                }
                QueryAlg query_alg(m_k);
                
                query_alg(cursors, block_end);
               
                auto& small_topk = query_alg.topk();
                // std::cout << m_topk.threshold() << std::endl;
                for (const auto &entry : small_topk) {
                    // if (entry.second > 128 && entry.second < 256) {
                    //     std::cout << entry.first << " " << entry.second << " " << live_dead[block] << std::endl;
                    // }
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
