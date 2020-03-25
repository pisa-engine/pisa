#pragma once

#include "util/util.hpp"
#include <algorithm>
#include <deque>
#include <iterator>
#include <vector>

namespace pisa {

using posting_t = uint32_t;
using wand_cost_t = float;

struct score_opt_partition {
    std::vector<uint32_t> partition;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> sizes;
    std::vector<float> max_values;
    std::vector<float> errors;
    wand_cost_t cost_opt = 0;  // the costs are in bits!

    template <typename ForwardIterator>
    struct score_window {
        // a window represent the cost of the interval [start, end)
        ForwardIterator start_it;
        ForwardIterator end_it;
        // starting and ending position of the window
        posting_t start = 0;
        posting_t end = 0;  // end-th position is not in the current window
        posting_t min_p = 0;
        posting_t max_p = 0;
        wand_cost_t cost_upper_bound;  // The maximum cost for this window
        std::deque<float> max_queue;
        float m_fixed_cost;
        float sum;
        uint64_t end_sequence;
        uint64_t element_count;

        score_window(
            ForwardIterator begin,
            posting_t base,
            wand_cost_t cost_upper_bound,
            float fixed_cost,
            size_t size)
            : start_it(begin),
              end_it(begin),
              min_p(base),
              max_p(0),
              cost_upper_bound(cost_upper_bound),
              m_fixed_cost(fixed_cost),
              sum(0),
              end_sequence(size),
              element_count(0)
        {}

        uint64_t universe() const { return max_p - min_p + 1; }

        uint64_t size() const { return end - start; }

        void advance_start()
        {
            float v = std::get<1>(*start_it);
            if (std::get<1>(*start_it) == max_queue.front()) {
                max_queue.pop_front();
            }

            sum -= v;
            ++start;
            ++start_it;
            if (std::get<1>(*start_it) != 0) {
                ++element_count;
            }
        }

        void advance_end()
        {
            float v = std::get<1>(*end_it);
            sum += v;

            while (not max_queue.empty() && max_queue.back() < std::get<1>(*end_it)) {
                max_queue.pop_back();
            }

            max_queue.push_back(std::get<1>(*end_it));
            // max_p = *end_it;
            ++end;
            ++end_it;
            if (std::get<1>(*end_it) != 0) {
                --element_count;
            }
        }

        float cost()
        {
            if (size() < 2) {
                return m_fixed_cost;
            }
            return size() * max_queue.front() - sum + m_fixed_cost;
        }

        float max() { return max_queue.front(); }
    };

    score_opt_partition() = default;

    template <typename ForwardIterator>
    score_opt_partition(
        ForwardIterator begin, uint32_t base, uint64_t size, double eps1, double eps2, float fixed_cost)
    {
        // compute cost of single block.
        float max = 0;
        float sum = 0;
        std::for_each(begin, begin + size, [&](auto& b) {
            max = std::max(max, std::get<1>(b));
            sum += std::get<1>(b);
        });

        wand_cost_t single_block_cost = size * max - sum;
        std::vector<wand_cost_t> min_cost(size + 1, single_block_cost);
        min_cost[0] = 0;

        // create the required window: one for each power of approx_factor
        std::vector<score_window<ForwardIterator>> windows;
        wand_cost_t cost_lb = fixed_cost;
        wand_cost_t cost_bound = cost_lb;
        while (eps1 == 0 || cost_bound < cost_lb / eps1) {
            windows.emplace_back(begin, base, cost_bound, fixed_cost, size);
            if (cost_bound >= single_block_cost) {
                break;
            }
            cost_bound = cost_bound * (1 + eps2);
        }

        std::vector<posting_t> path(size + 1, 0);
        std::vector<float> maxs(size + 1, 0);

        auto max1 = std::max_element(
            begin, begin + size, [](const auto& lhs, const auto& rhs) -> auto {
                return std::get<1>(lhs) < std::get<1>(rhs);
            });
        maxs[size] = std::get<1>(*max1);

        for (posting_t i = 0; i < size; i++) {
            size_t last_end = i + 1;
            for (auto& window: windows) {
                assert(window.start == i);
                while (window.end < last_end) {
                    window.advance_end();
                }

                wand_cost_t window_cost;
                while (true) {
                    window_cost = window.cost();

                    if ((min_cost[i] + window_cost < min_cost[window.end])) {
                        min_cost[window.end] = min_cost[i] + window_cost;
                        path[window.end] = window.start;
                        maxs[window.end] = window.max();
                    }
                    last_end = window.end;
                    if (window.end == size) {
                        break;
                    }
                    if (window_cost >= window.cost_upper_bound) {
                        break;
                    }
                    window.advance_end();
                }

                window.advance_start();
            }
        }

        posting_t curr_pos = size;
        std::vector<float> max_values_temp;
        while (curr_pos != 0) {
            partition.push_back(curr_pos);
            max_values_temp.push_back(maxs[curr_pos]);
            errors.push_back(min_cost[curr_pos] / ((float)curr_pos / path[curr_pos]));
            curr_pos = path[curr_pos];
        }

        std::reverse(partition.begin(), partition.end());
        std::reverse(max_values_temp.begin(), max_values_temp.end());
        std::reverse(errors.begin(), errors.end());

        uint32_t current = 0;
        for (size_t i = 0; i < partition.size() - 1; i++) {
            docids.push_back(std::get<0>(*(begin + partition[i])) - 1);
            max_values.push_back(max_values_temp[i]);
            sizes.push_back(partition[i] - current);
            current = partition[i];
        }

        sizes.push_back(partition[partition.size() - 1] - current);
        max_values.push_back(max_values_temp[partition.size() - 1]);
        docids.push_back(std::get<0>(*(begin + size - 1)));
        cost_opt = min_cost[size];
    }
};

}  // namespace pisa
