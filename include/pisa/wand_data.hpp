#pragma once

#include <algorithm>
#include <numeric>

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "mappable/mappable_vector.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"
#include "wand_data_raw.hpp"

#include "scorer/scorer.hpp"

class enumerator;
namespace pisa {

template <typename block_wand_type = wand_data_raw>
class wand_data {
   public:
    using wand_data_enumerator = typename block_wand_type::enumerator;

    wand_data() {}

    template <typename LengthsIterator>
    wand_data(LengthsIterator len_it,
              uint64_t num_docs,
              binary_freq_collection const &coll,
              std::string const &scorer_name,
              BlockSize block_size) : m_num_docs(num_docs)
    {
        std::vector<uint32_t> doc_lens(num_docs);
        std::vector<float> max_term_weight;
        std::vector<uint32_t> term_occurrence_counts;
        std::vector<uint32_t> term_lens;
        global_parameters params;
        spdlog::info("Reading sizes...");

        for (size_t i = 0; i < num_docs; ++i) {
            uint32_t len = *len_it++;
            doc_lens[i] = len;
            m_collection_len += len;
        }

        m_avg_len = float(m_collection_len / double(num_docs));

        typename block_wand_type::builder builder(coll, params);

        for (auto const &seq : coll) {
            size_t term_occurrence_count = std::accumulate(seq.freqs.begin(), seq.freqs.end(), 0);
            term_occurrence_counts.push_back(term_occurrence_count);
            term_lens.push_back(seq.docs.size());
        }
        m_doc_lens.steal(doc_lens);
        m_term_occurrence_counts.steal(term_occurrence_counts);
        m_term_lens.steal(term_lens);

        auto scorer = scorer::from_name(scorer_name, *this);
        {
            pisa::progress progress("Processing posting lists", coll.size());
            size_t term_id = 0;
            for (auto const &seq : coll) {
                auto v = builder.add_sequence(
                    seq, coll, doc_lens, m_avg_len, scorer->term_scorer(term_id), block_size);
                max_term_weight.push_back(v);
                term_id += 1;
                progress.update(1);
            }
        }
        builder.build(m_block_wand);
        m_max_term_weight.steal(max_term_weight);
    }

    float norm_len(uint64_t doc_id) const { return m_doc_lens[doc_id] / m_avg_len; }

    size_t doc_len(uint64_t doc_id) const { return m_doc_lens[doc_id]; }

    size_t term_occurrence_count(uint64_t term_id) const { return m_term_occurrence_counts[term_id]; }

    size_t term_len(uint64_t term_id) const { return m_term_lens[term_id]; }

    size_t num_docs() const { return m_num_docs; }

    float avg_len() const { return m_avg_len; }

    uint64_t collection_len() const { return m_collection_len; }

    float max_term_weight(uint64_t list) const { return m_max_term_weight[list]; }

    wand_data_enumerator getenum(size_t i) const
    {
        return m_block_wand.get_enum(i, max_term_weight(i));
    }

    const block_wand_type &get_block_wand() const { return m_block_wand; }

    template <typename Visitor>
    void map(Visitor &visit)
    {
        visit(m_block_wand, "m_block_wand")(m_doc_lens, "m_doc_lens")(
            m_term_occurrence_counts, "m_term_occurrence_counts")(m_term_lens, "m_term_lens")(m_avg_len, "m_avg_len")(
            m_collection_len, "m_collection_len")(m_num_docs, "m_num_docs")(m_max_term_weight,
                                                                            "m_max_term_weight");
    }

   private:
    uint64_t m_num_docs = 0;
    float m_avg_len = 0;
    uint64_t m_collection_len = 0;
    block_wand_type m_block_wand;
    mapper::mappable_vector<uint32_t> m_doc_lens;
    mapper::mappable_vector<uint32_t> m_term_occurrence_counts;
    mapper::mappable_vector<uint32_t> m_term_lens;
    mapper::mappable_vector<float> m_max_term_weight;
};
} // namespace pisa
