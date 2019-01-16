#pragma once

namespace pisa {

template <int block_size>
struct Blocked_Accumulator {

    struct Proxy_Element {
        std::ptrdiff_t      document;
        std::vector<float> &accumulators;
        std::vector<float> &accumulators_max;

        Proxy_Element &operator=(float score) {
            accumulators[document] = score;
            auto &block_max        = accumulators_max[document / block_size];
            if (score > block_max) {
                block_max = score;
            }
            return *this;
        }
        Proxy_Element &operator+=(float delta) {
            accumulators[document] += delta;
            auto const&score = accumulators[document];
            auto &block_max = accumulators_max[document / block_size];
            if (score > block_max) {
                block_max = score;
            }
            return *this;
        }

        operator float() { return accumulators[document]; }
    };

    using reference = Proxy_Element;

    static_assert(block_size > 0, "must be positive");

    [[nodiscard]] constexpr static auto calc_block_count(std::size_t size) noexcept -> std::size_t {
        return (size + block_size - 1) / block_size;
    }

    Blocked_Accumulator(std::size_t size)
        : m_size(size),
        m_block_count(calc_block_count(size)), m_accumulators(size),
        m_accumulators_max(m_block_count) {}

    void init() { std::fill(m_accumulators.begin(), m_accumulators.end(), 0.0); }

    [[nodiscard]] auto operator[](std::ptrdiff_t document) -> Proxy_Element
    {
        return {document, m_accumulators, m_accumulators_max};
    }

    void accumulate(std::ptrdiff_t const document, float score_delta)
    {
        m_accumulators[document] += score_delta;
        auto const &score = m_accumulators[document];
        auto &block_max = m_accumulators_max[document / block_size];
        if (score > block_max) {
            block_max = score;
        }
    }

    void aggregate(topk_queue &topk) {
        for (size_t block = 0; block < m_block_count; ++block) {
            if (not topk.would_enter(m_accumulators_max[block])) { continue; }
            uint32_t doc = block * block_size;
            uint32_t end = std::min((block + 1) * block_size, m_accumulators.size());
            for (; doc < end; ++doc) {
                topk.insert(m_accumulators[doc], doc);
            }
        }
    }

    [[nodiscard]] auto size() noexcept -> std::size_t { return m_size; }

   private:
    std::size_t        m_size;
    std::size_t        m_block_count;
    std::vector<float> m_accumulators;
    std::vector<float> m_accumulators_max;
};

} // pisa
