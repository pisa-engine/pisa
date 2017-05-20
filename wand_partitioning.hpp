#pragma once

#include <vector>
#include <algorithm>
#include <iterator>
#include <deque>
#include "util.hpp"


/**
 * FIXME Creare classe unica con score partitioning e wand partitioning.
 */


namespace ds2i {

    typedef uint32_t posting_t ;
    typedef float wand_cost_t;

    struct wand_opt_partition {

        std::vector<uint32_t> partition;
        std::vector<float> max_values;
        wand_cost_t cost_opt = 0; // the costs are in bits!

        template <typename ForwardIterator, typename SizeIterator>
        struct wand_window {
            // a window represent the cost of the interval [start, end)

            ForwardIterator start_it;
            ForwardIterator end_it;

            SizeIterator startsize;
            SizeIterator endsize;

            // starting and ending position of the window
            posting_t start = 0;
            posting_t end = 0; // end-th position is not in the current window
            posting_t min_p = 0;
            posting_t max_p = 0;
            wand_cost_t cost_upper_bound; // The maximum cost for this window
            std::deque<float> max_queue;
            float m_fixed_cost;
            float sum;
            uint64_t end_sequence;
            uint64_t m_block_size;

            wand_window(ForwardIterator begin, SizeIterator begin_size, posting_t base, wand_cost_t cost_upper_bound, float fixed_cost, size_t size)
                    : start_it(begin)
                    , end_it(begin)
                    , startsize(begin_size)
                    , endsize(begin_size)
                    , min_p(base)
                    , max_p(0)
                    , cost_upper_bound(cost_upper_bound)
                    , m_fixed_cost(fixed_cost)
                    , sum(0)
                    , end_sequence(size)
                    , m_block_size(0)
            {}

            uint64_t universe() const {
                return max_p - min_p + 1;
            }

            uint64_t size() const {
                return end - start;
            }

            uint64_t block_size() const {
                return m_block_size;
            }

            void advance_start() {

                while (*start_it == -1 and start != end ) {
                    start_it++;
                    startsize++;
                    start++;
                }

                if (*start_it == max_queue.front())
                    max_queue.pop_front();

                sum -= *start_it * *startsize;
                m_block_size -= *startsize;


                min_p = *start_it + 1;
                ++start;
                ++start_it;
                ++startsize;
            }

            void advance_end() {

                while (*end_it == -1 and end != end_sequence - 1) {
                    end++;
                    end_it++;
                    endsize++;
                }

                if (*end_it == -1){
                    end++;
                    return;
                }

                sum += *end_it * *endsize;


                while (max_queue.size() > 0 && max_queue.back() < *end_it){
                    max_queue.pop_back();
                }

                max_queue.push_back(*end_it);
                max_p = *end_it;

                m_block_size += *endsize;

                ++end;
                ++end_it;
                ++endsize;
            }

            float cost(){
                if (size() < 2)
                    return m_fixed_cost;
                else 
                    return block_size()*max_queue.front()  - sum + m_fixed_cost;
            }

            float max(){
                return max_queue.front();
            }

        };

        template <typename ForwardIterator, typename SizeIterator>
         wand_opt_partition(ForwardIterator begin, SizeIterator begin_size,
                          uint32_t base, uint64_t size, double eps1, double eps2, float fixed_cost)
        {


            // compute cost of single block.
            ForwardIterator b = begin;
            SizeIterator s = begin_size;
            float max = 0;
            float sum = 0;
            size_t bsize = 0;
            for (size_t i = 0; i < size; i++){
                if (*b > max)
                    max = *b;
                sum += *b * *s;
                bsize += *s;
                b++;
                s++;
            }

//            assert (size*max >= sum);
            wand_cost_t single_block_cost = bsize*max - sum;
            std::vector<wand_cost_t> min_cost(size + 1, single_block_cost);
            min_cost[0] = 0;

            // create the required window: one for each power of approx_factor
            std::vector<wand_window<ForwardIterator, SizeIterator>> windows;
            wand_cost_t cost_lb = fixed_cost;
            wand_cost_t cost_bound = cost_lb;
            while (eps1 == 0 || cost_bound < cost_lb / eps1) {
                windows.emplace_back(begin, begin_size, base, cost_bound, fixed_cost, size);
                if (cost_bound >= single_block_cost) break;
                cost_bound = cost_bound * (1 + eps2);
            }

            std::vector<posting_t> path(size + 1, 0);
            std::vector<float> maxs(size + 1, 0);

            //fixme provvisorio

            float max1 = 0;
            ForwardIterator ittemp = begin;
            for (uint64_t i = 0; i < size; i++, ittemp++){
                if (*ittemp > max1)
                    max1 = *ittemp;
            }

            maxs[size] = max1;

            for (posting_t i = 0; i < size; i++) {
                size_t last_end = i + 1;
                for (auto& window: windows) {

                    if (i != window.start)
                        continue;
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
                        if (window.end == size) break;
                        if (window_cost >= window.cost_upper_bound) break;
                        window.advance_end();
                    }

                    window.advance_start();
                }
            }

            posting_t curr_pos = size;
            while( curr_pos != 0 ) {
                partition.push_back(curr_pos);
                max_values.push_back(maxs[curr_pos]);
                curr_pos = path[curr_pos];
            }

            std::reverse(partition.begin(), partition.end());
            std::reverse(max_values.begin(), max_values.end());
            cost_opt = min_cost[size];
        }
    };

}

