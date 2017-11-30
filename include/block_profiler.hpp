#pragma once

#include <iostream>
#include <atomic>
#include <mutex>

namespace ds2i {

    class block_profiler {
    public:

        ~block_profiler()
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            for (auto const& it: m_block_freqs) {
                delete [] it.second.second;
            }
        }

        typedef std::atomic_uint_fast32_t counter_type;

        static block_profiler& get() {
            static block_profiler instance;
            return instance;
        }

        static counter_type* open_list(uint32_t term_id, uint32_t blocks)
        {
            block_profiler& instance = get();
            std::lock_guard<std::mutex> lock(instance.m_mutex);
            auto& v = instance.m_block_freqs[term_id];
            if (v.second == nullptr) {
                v.first = 2 * blocks;
                v.second = new counter_type[v.first];
                std::fill(v.second, v.second + v.first, 0);
            }
            return v.second;
        }

        static void dump(std::ostream& os)
        {
            block_profiler& instance = get();
            std::lock_guard<std::mutex> lock(instance.m_mutex);

            for (auto const& it: instance.m_block_freqs) {
                os << it.first;

                for (size_t i = 0; i < it.second.first; ++i) {
                    os << '\t' << it.second.second[i];
                }

                os << '\n';
            }
        }

    private:
        block_profiler() {}

        // XXX can't do vector of atomics ARGHH
        std::map<uint32_t, std::pair<size_t, counter_type*>> m_block_freqs;
        std::mutex m_mutex;
    };

}
