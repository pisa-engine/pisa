#pragma once

#include "bit_vector.hpp"
#include "query/queries.hpp"
#include "topk_queue.hpp"
#include <vector>
namespace pisa {

template <size_t range_size>
struct block_max_wand_lb_query {
    explicit block_max_wand_lb_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(
        CursorRange&& cursors, uint64_t max_docid, bit_vector const& live_blocks)
    {
        using Cursor = typename std::decay_t<CursorRange>::value_type;
        if (cursors.empty()) {
            return;
        }

        std::vector<Cursor*> ordered_cursors;
        ordered_cursors.reserve(cursors.size());
        for (auto& en: cursors) {
            ordered_cursors.push_back(&en);
        }

        auto sort_cursors = [&]() {
            // sort enumerators by increasing docid
            std::sort(ordered_cursors.begin(), ordered_cursors.end(), [](Cursor* lhs, Cursor* rhs) {
                return lhs->docid() < rhs->docid();
            });
        };

        bit_vector::unary_enumerator en(live_blocks, 0);
        auto live_block = en.next();

        uint32_t nextLiveDid = live_block * range_size;
        for (Cursor* en: ordered_cursors) {
            en->next_geq(nextLiveDid);
        }

        sort_cursors();

        while (true) {
            // find pivot
            float upper_bound = 0.F;
            size_t pivot;
            bool found_pivot = false;
            uint64_t pivot_id = max_docid;

            for (pivot = 0; pivot < ordered_cursors.size(); ++pivot) {
                if (ordered_cursors[pivot]->docid() >= max_docid) {
                    break;
                }

                upper_bound += ordered_cursors[pivot]->max_score();
                if (m_topk.would_enter(upper_bound)) {
                    found_pivot = true;
                    pivot_id = ordered_cursors[pivot]->docid();
                    for (; pivot + 1 < ordered_cursors.size()
                         && ordered_cursors[pivot + 1]->docid() == pivot_id;
                         ++pivot) {
                    }
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }

            double block_upper_bound = 0;

            for (size_t i = 0; i < pivot + 1; ++i) {
                block_upper_bound += ordered_cursors[i]->scores(pivot_id / range_size);
            }

            if (m_topk.would_enter(block_upper_bound)) {
                // check if pivot is a possible match
                if (pivot_id == ordered_cursors[0]->docid()) {
                    float score = 0;
                    for (Cursor* en: ordered_cursors) {
                        if (en->docid() != pivot_id) {
                            break;
                        }
                        float part_score = en->score();
                        score += part_score;
                        block_upper_bound -= en->scores(pivot_id / range_size) - part_score;
                        if (!m_topk.would_enter(block_upper_bound)) {
                            break;
                        }
                    }
                    if (pivot_id + 1 < (ordered_cursors[pivot]->docid()/range_size + 1) * range_size) {
                        nextLiveDid = pivot_id + 1;
                    } else {
	     		        bit_vector::unary_enumerator en(live_blocks, ordered_cursors[pivot]->docid()/range_size);
				        live_block = en.next();
                        nextLiveDid = std::min(max_docid,std::max(pivot_id + 1, live_block * range_size));
                    }

                    for (Cursor* en: ordered_cursors) {
                        if (en->docid() != pivot_id) {
                            break;
                        }
                        en->next_geq(nextLiveDid);
                    }

                    m_topk.insert(score, pivot_id);
                    // resort by docid
                    sort_cursors();

                } else {
                    uint64_t next_list = pivot;
                    for (; ordered_cursors[next_list]->docid() == pivot_id; --next_list) {
                    }
                    ordered_cursors[next_list]->next_geq(pivot_id);

                    // bubble down the advanced list
                    for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                        if (ordered_cursors[i]->docid() <= ordered_cursors[i - 1]->docid()) {
                            std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                        } else {
                            break;
                        }
                    }
                }

            } else {
                uint64_t next = max_docid;
                uint64_t next_list = pivot;

                float max_weight = ordered_cursors[next_list]->max_score();

                for (uint64_t i = 0; i < pivot; i++) {
                    if (ordered_cursors[i]->max_score() > max_weight) {
                        next_list = i;
                        max_weight = ordered_cursors[i]->max_score();
                    }
                }

                size_t block = ordered_cursors[pivot]->docid()/range_size;
                uint32_t smallest_did = (block + 1) * range_size;
                next = smallest_did;
                if (pivot + 1 < ordered_cursors.size() && ordered_cursors[pivot + 1]->docid() < smallest_did) {
                    next = ordered_cursors[pivot + 1]->docid();
                }
				if (next <= pivot_id) {
                    next = pivot_id + 1;
                }

		        bit_vector::unary_enumerator en(live_blocks, block);
		        live_block = en.next();
				next = std::min(max_docid,std::max(next, live_block * range_size));

                ordered_cursors[next_list]->next_geq(next);

                // bubble down the advanced list
                for (size_t i = next_list + 1; i < ordered_cursors.size(); ++i) {
                    if (ordered_cursors[i]->docid() < ordered_cursors[i - 1]->docid()) {
                        std::swap(ordered_cursors[i], ordered_cursors[i - 1]);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

    void clear_topk() { m_topk.clear(); }

    topk_queue const& get_topk() const { return m_topk; }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa
