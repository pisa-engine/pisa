#pragma once

#include "util/intrinsics.hpp"
#include "topk_queue.hpp"

namespace pisa {

using score_function_type = std::function<float(uint64_t, uint64_t)>;

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
        score_functions.push_back([q_weight, &wdata](auto docid, auto freq) {
            float norm_len = wdata.norm_len(docid);
            return q_weight * scorer_type::doc_term_weight(freq, norm_len);
        });
    }
    return std::make_pair(cursors, score_functions);
}

} // namespace query
template <int counter_bit_size, typename Descriptor = std::uint64_t>
struct Lazy_Accumulator {
    static_assert(std::is_integral_v<Descriptor> && std::is_unsigned_v<Descriptor>,
                  "must be unsigned number");
    constexpr static auto descriptor_size        = sizeof(Descriptor);
    constexpr static auto counters_in_descriptor = descriptor_size / counter_bit_size;
    constexpr static auto mask                   = (1u << counter_bit_size) - 1;
    constexpr static auto cycle                  = (1u << counter_bit_size);

    struct Block {
        Descriptor                                descriptor{};
        std::array<float, counters_in_descriptor> accumulators{};

        [[nodiscard]] auto counter(int pos) -> int {
            return (descriptor >> (pos * counter_bit_size)) & mask;
        }
    };

    Lazy_Accumulator(std::size_t size)
        : m_size(size),
          m_accumulators((size + counters_in_descriptor - 1) / counters_in_descriptor) {}

    void init() {
        if (m_counter == 0) {
            auto first = reinterpret_cast<std::byte *>(&m_accumulators.front());
            auto last =
                std::next(reinterpret_cast<std::byte *>(&m_accumulators.back()), sizeof(Block));
            std::fill(first, last, std::byte{});
        }
    }

    float &operator[](std::ptrdiff_t document) {
        auto block        = document / counters_in_descriptor;
        auto pos_in_block = document % counters_in_descriptor;
        if (m_accumulators[block].counter(pos_in_block) < m_counter) {
            m_accumulators[block].descriptor ^= (mask << pos_in_block * counter_bit_size);
            m_accumulators[block].accumulators[pos_in_block] = 0;
        }
        return m_accumulators[block].accumulators[pos_in_block];
    }

    void aggregate(topk_queue &topk) {
        uint64_t docid = 0u;
        for (auto const &block : m_accumulators) {
            for (auto const &score : block.accumulators) {
                topk.insert(score, docid++);
            }
        };
        m_counter = (m_counter + 1) % cycle;
    }

    [[nodiscard]] auto size() noexcept -> std::size_t { return m_size; }

   private:
    std::size_t        m_size;
    std::vector<Block> m_accumulators;
    int                m_counter{};
};

struct Simple_Accumulator : public std::vector<float> {
    Simple_Accumulator(std::ptrdiff_t size) : std::vector<float>(size) {}
    void init() { std::fill(begin(), end(), 0.0); }
    void aggregate(topk_queue &topk) {
        uint64_t docid = 0u;
        std::for_each(begin(), end(), [&](auto score) { topk.insert(score, docid++); });
    }
};

struct Taat_Traversal {
    template <typename Cursor, typename Acc>
    void static traverse_term(Cursor &cursor, score_function_type score, Acc &acc) {
        if constexpr (std::is_same_v<typename Cursor::enumerator_category,
                                     ds2i::block_enumerator_tag>) {
            while (cursor.docid() < acc.size()) {
                auto const &documents = cursor.document_buffer();
                auto const &freqs     = cursor.frequency_buffer();
                for (uint32_t idx = 0; idx < documents.size(); ++idx) {
                    acc[documents[idx]] = score(documents[idx], freqs[idx]);
                }
                cursor.next_block();
            }
        } else {
            for (; cursor.docid() < acc.size(); cursor.next()) {
                acc[cursor.docid()] = score(cursor.docid(), cursor.freq());
            }
        }
    }
};

template <typename Index, typename WandType, typename Acc = Simple_Accumulator>
class exhaustive_taat_query {
   public:
    exhaustive_taat_query(Index const &index, WandType const &wdata, uint64_t k)
        : m_index(index), m_wdata(wdata), m_topk(k), m_accumulators(index.num_docs()) {}

    uint64_t operator()(term_id_vec terms) {
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
