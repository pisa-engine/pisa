#pragma once

#include <numeric>

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "mappable/mappable_vector.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"
#include "wand_data_raw.hpp"

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
              BlockSize block_size)
    {
        std::vector<uint32_t> doc_lens(num_docs);
        std::vector<float> max_term_weight;
        std::vector<uint32_t> terms_count;
        global_parameters params;
        double collection_len = 0;
        spdlog::info("Reading sizes...");

        for (size_t i = 0; i < num_docs; ++i) {
            float len = *len_it++;
            doc_lens[i] = len;
            collection_len += len;
        }

        float avg_len = float(collection_len / double(num_docs));

        typename block_wand_type::builder builder(coll, params);
        {
            pisa::progress progress("Processing posting lists", coll.size());
            for (auto const &seq : coll) {
                size_t term_count = std::accumulate(seq.freqs.begin(), seq.freqs.end(), 0);
                terms_count.push_back(term_count);
                auto v = builder.add_sequence(seq, coll, doc_lens, avg_len, block_size);
                max_term_weight.push_back(v);
                progress.update(1);
            }
        }
        builder.build(m_block_wand);
        m_doc_lens.steal(doc_lens);
        m_max_term_weight.steal(max_term_weight);
        m_terms_count.steal(terms_count);
        m_avg_len = avg_len;
        m_collection_len = collection_len;
    }

    inline float norm_len(uint64_t doc_id) const { return m_doc_lens[doc_id] / m_avg_len; }

    size_t doc_len(uint64_t doc_id) const { return m_doc_lens[doc_id]; }

    size_t term_count(uint64_t term_id) const { return m_terms_count[term_id]; }

    float avg_len() const { return m_avg_len; }

    uint64_t collection_len() const { return m_collection_len; }

    float max_term_weight(uint64_t list) const { return m_max_term_weight[list]; }

    wand_data_enumerator getenum(size_t i) const { return m_block_wand.get_enum(i); }

    const block_wand_type &get_block_wand() const { return m_block_wand; }

    template <typename Visitor>
    void map(Visitor &visit)
    {
        visit(m_block_wand, "m_block_wand")(m_doc_lens, "m_doc_lens")(
            m_terms_count, "m_terms_count")(m_avg_len, "m_avg_len")(
            m_collection_len, "m_collection_len")(m_max_term_weight, "m_max_term_weight");
    }

   private:
    block_wand_type m_block_wand;
    mapper::mappable_vector<uint32_t> m_doc_lens;
    mapper::mappable_vector<uint32_t> m_terms_count;
    float m_avg_len;
    uint64_t m_collection_len;
    mapper::mappable_vector<float> m_max_term_weight;
};
} // namespace pisa
