#pragma once

#include <atomic>
#include <iostream>
#include <map>
#include <mutex>

namespace pisa {

class block_profiler {
  public:
    block_profiler(block_profiler const&) = delete;
    block_profiler(block_profiler&&) = delete;
    block_profiler operator=(block_profiler const&) = delete;
    block_profiler operator=(block_profiler&&) = delete;
    ~block_profiler()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto const& it: m_block_freqs) {
            delete[] it.second.second;
        }
    }

    using counter_type = std::atomic_uint_fast32_t;

    static block_profiler& get()
    {
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
    block_profiler() = default;

    // XXX can't do vector of atomics ARGHH
    std::map<uint32_t, std::pair<size_t, counter_type*>> m_block_freqs;
    std::mutex m_mutex;
};

}  // namespace pisa
