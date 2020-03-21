#pragma once

#include "util/util.hpp"
#include <algorithm>
#include <iterator>
#include <vector>

namespace pisa {

using posting_t = uint32_t;
using cost_t = uint64_t;

struct optimal_partition {
    std::vector<posting_t> partition;
    cost_t cost_opt = 0;  // the costs are in bits!

    template <typename ForwardIterator>
    struct cost_window {
        // a window reppresent the cost of the interval [start, end)
        ForwardIterator start_it;
        ForwardIterator end_it;
        // starting and ending position of the window
        posting_t start = 0;
        posting_t end = 0;  // end-th position is not in the current window
        posting_t min_p = 0;  // element that preceed the first element of the window
        posting_t max_p = 0;

        cost_t cost_upper_bound;  // The maximum cost for this window

        cost_window(ForwardIterator begin, posting_t base, cost_t cost_upper_bound)
            : start_it(begin), end_it(begin), min_p(base), max_p(0), cost_upper_bound(cost_upper_bound)
        {}

        uint64_t universe() const { return max_p - min_p + 1; }

        uint64_t size() const { return end - start; }

        void advance_start()
        {
            min_p = *start_it + 1;
            ++start;
            ++start_it;
        }

        void advance_end()
        {
            max_p = *end_it;
            ++end;
            ++end_it;
        }
    };

    optimal_partition() = default;

    template <typename ForwardIterator, typename CostFunction>
    optimal_partition(
        ForwardIterator begin,
        posting_t base,
        posting_t universe,
        uint64_t size,
        CostFunction cost_fun,
        double eps1,
        double eps2)
    {
        cost_t single_block_cost = cost_fun(universe - base, size);
        std::vector<cost_t> min_cost(size + 1, single_block_cost);
        min_cost[0] = 0;

        // create the required window: one for each power of approx_factor
        std::vector<cost_window<ForwardIterator>> windows;
        cost_t cost_lb = cost_fun(1, 1);  // minimum cost
        cost_t cost_bound = cost_lb;
        while (eps1 == 0 || cost_bound < cost_lb / eps1) {
            windows.emplace_back(begin, base, cost_bound);
            if (cost_bound >= single_block_cost) {
                break;
            }
            cost_bound = cost_bound * (1 + eps2);
        }

        std::vector<posting_t> path(size + 1, 0);
        for (posting_t i = 0; i < size; i++) {
            size_t last_end = i + 1;
            for (auto& window: windows) {
                assert(window.start == i);
                while (window.end < last_end) {
                    window.advance_end();
                }

                cost_t window_cost;
                while (true) {
                    window_cost = cost_fun(window.universe(), window.size());
                    if ((min_cost[i] + window_cost < min_cost[window.end])) {
                        min_cost[window.end] = min_cost[i] + window_cost;
                        path[window.end] = i;
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
        while (curr_pos != 0) {
            partition.push_back(curr_pos);
            curr_pos = path[curr_pos];
        }
        std::reverse(partition.begin(), partition.end());
        cost_opt = min_cost[size];
    }
};

}  // namespace pisa
