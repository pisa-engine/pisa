#pragma once

#include "util/intrinsics.hpp"
#include "topk_queue.hpp"

namespace pisa {

template <typename Scorer, typename Wand>
struct Score_Function {
    float query_weight;
    std::reference_wrapper<Wand const> wdata;

    [[nodiscard]] auto operator()(uint32_t doc, uint32_t freq) const -> float {
        return query_weight * Scorer::doc_term_weight(freq, wdata.get().norm_len(doc));
    }
};

// TODO: These are functions common to query processing in general.
//       They should be moved out of this file.
namespace query {

template <typename Index, typename WandType>
[[nodiscard]] auto cursors_with_scores(Index const& index, WandType const &wdata, term_id_vec terms)
{
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type         = bm25;
    using cursor_type         = typename Index::document_enumerator;
    using score_function_type = Score_Function<scorer_type, WandType>;

    auto query_term_freqs = query_freqs(terms);
    std::vector<cursor_type> cursors;
    std::vector<score_function_type> score_functions;
    cursors.reserve(query_term_freqs.size());
    score_functions.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto     list     = index[term.first];
        uint64_t num_docs = index.num_docs();
        auto     q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
        cursors.push_back(std::move(list));
        score_functions.push_back({q_weight, std::cref(wdata)});
    }
    return std::make_pair(cursors, score_functions);
}

} // namespace query

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

template <int counter_bit_size, typename Descriptor = std::uint64_t>
struct Lazy_Accumulator {
    using reference = float &;

    static_assert(std::is_integral_v<Descriptor> && std::is_unsigned_v<Descriptor>,
                  "must be unsigned number");
    constexpr static auto descriptor_size_in_bits = sizeof(Descriptor) * 8;
    constexpr static auto counters_in_descriptor = descriptor_size_in_bits / counter_bit_size;
    constexpr static auto cycle = (1u << counter_bit_size);
    constexpr static Descriptor mask = (1u << counter_bit_size) - 1;

    struct Block {
        Descriptor                                descriptor{};
        std::array<float, counters_in_descriptor> accumulators{};

        [[nodiscard]] auto counter(int pos) const noexcept -> int {
            return (descriptor >> (pos * counter_bit_size)) & mask;
        }

        void reset_counter(int pos, int counter)
        {
            auto const shift = pos * counter_bit_size;
            descriptor &= ~(mask << shift);
            descriptor |= static_cast<Descriptor>(counter) << shift;
            accumulators[pos] = 0;
        }
    };

    Lazy_Accumulator(std::size_t size)
        : m_size(size), m_accumulators((size + counters_in_descriptor - 1) / counters_in_descriptor)
    {}

    void init()
    {
        if (m_counter == 0) {
            auto first = reinterpret_cast<std::byte *>(&m_accumulators.front());
            auto last =
                std::next(reinterpret_cast<std::byte *>(&m_accumulators.back()), sizeof(Block));
            std::fill(first, last, std::byte{0});
        }
    }

    float &operator[](std::ptrdiff_t const document) {
        auto const block        = document / counters_in_descriptor;
        auto const pos_in_block = document % counters_in_descriptor;
        if (//m_accumulators[block].accumulators[pos_in_block] > 0 &&
            m_accumulators[block].counter(pos_in_block) != m_counter)
        {
            auto const shift = pos_in_block * counter_bit_size;
            m_accumulators[block].descriptor &= ~(mask << shift);
            m_accumulators[block].descriptor |= m_counter << shift;
            m_accumulators[block].accumulators[pos_in_block] = 0;
        }
        return m_accumulators[block].accumulators[pos_in_block];
    }

    void accumulate(std::ptrdiff_t const document, float score)
    {
        auto const block = document / counters_in_descriptor;
        auto const pos_in_block = document % counters_in_descriptor;
        if (m_accumulators[block].counter(pos_in_block) != m_counter) {
            m_accumulators[block].reset_counter(pos_in_block, m_counter);
        }
        m_accumulators[block].accumulators[pos_in_block] += score;
    }

    void aggregate(topk_queue &topk) {
        uint64_t docid = 0u;
        for (auto const &block : m_accumulators) {
            int pos = 0;
            for (auto const &score : block.accumulators) {
                if (block.counter(pos++) == m_counter) {
                    topk.insert(score, docid);
                }
                ++docid;
            }
        };
        m_counter = (m_counter + 1) % cycle;
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_size; }
    [[nodiscard]] auto blocks() noexcept -> std::vector<Block> & { return m_accumulators; }
    [[nodiscard]] auto counter() const noexcept -> int { return m_counter; }

   private:
    std::size_t        m_size;
    std::vector<Block> m_accumulators;
    int                m_counter{};
};

struct Simple_Accumulator : public std::vector<float> {
    Simple_Accumulator(std::ptrdiff_t size) : std::vector<float>(size) {}
    void init() { std::fill(begin(), end(), 0.0); }
    void accumulate(uint32_t doc, float score) { operator[](doc) += score; }
    void aggregate(topk_queue &topk) {
        uint64_t docid = 0u;
        std::for_each(begin(), end(), [&](auto score) { topk.insert(score, docid++); });
    }
};

struct Taat_Traversal {
    template <typename Cursor, typename Acc, typename Score>
    void static traverse_term(Cursor &cursor, Score score, Acc &acc)
    {
        if constexpr (std::is_same_v<typename Cursor::enumerator_category,
                                     pisa::block_enumerator_tag>) {
            while (cursor.docid() < acc.size()) {
                auto const &documents = cursor.document_buffer();
                auto const &freqs     = cursor.frequency_buffer();
                for (uint32_t idx = 0; idx < documents.size(); ++idx) {
                    acc.accumulate(documents[idx], score(documents[idx], freqs[idx] + 1));
                }
                cursor.next_block();
            }
        } else {
            for (; cursor.docid() < acc.size(); cursor.next()) {
                acc.accumulate(cursor.docid(), score(cursor.docid(), cursor.freq()));
            }
        }
    }
};

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class exhaustive_taat_query {
    using score_function_type = Score_Function<bm25, WandType>;

   public:
    exhaustive_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return taat(std::move(cws.first), std::move(cws.second));
    }

    uint64_t operator()([[maybe_unused]] Index const &, term_id_vec terms) {
        auto cws = query::cursors_with_scores(m_index, m_wdata, terms);
        return taat(std::move(cws.first), std::move(cws.second));
    }

    // TODO(michal): I think this should be eventually the `operator()`
    template <typename Cursor>
    uint64_t taat(std::vector<Cursor> cursors, std::vector<score_function_type> score_functions) {
        m_topk.clear();
        if (cursors.empty()) {
            return 0;
        }
        m_accumulators.init();
        for (uint32_t term = 0; term < cursors.size(); ++term) {
            Taat_Traversal::traverse_term(cursors[term], score_functions[term], m_accumulators);
        }
        m_accumulators.aggregate(m_topk);
        m_topk.finalize();
        return m_topk.topk().size();
    }

    std::vector<std::pair<float, uint64_t>> const &topk() const { return m_topk.topk(); }

   private:
    Index const &          m_index;
    WandType const &       m_wdata;
    topk_queue             m_topk;
    Acc                    m_accumulators;
};

template <typename Acc, typename Index, typename WandType>
[[nodiscard]] auto make_exhaustive_taat_query(Index const &   index,
                                              WandType const &wdata,
                                              uint64_t        k) {
    return exhaustive_taat_query<Index, WandType, Acc>(index, wdata, k);
}

}; // namespace pisa
