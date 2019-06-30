#pragma once
#include <numeric>
#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "mappable/mappable_vector.hpp"
#include "scorer/bm25.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"
#include "wand_data_raw.hpp"

class enumerator;
namespace pisa {

template <typename Scorer = bm25, typename block_wand_type = wand_data_raw<bm25>>
class wand_data {
   public:
    using wand_data_enumerator = typename block_wand_type::enumerator;

    wand_data() {}

    template <typename LengthsIterator>
    wand_data(LengthsIterator len_it, uint64_t num_docs, binary_freq_collection const &coll,
              BlockSize block_size) {
        std::vector<float>    doc_lens(num_docs);
        std::vector<float>    max_term_weight;
        std::vector<uint32_t> collection_terms_count;
        global_parameters     params;
        double                lens_sum = 0;
        spdlog::info("Reading sizes...");

        for (size_t i = 0; i < num_docs; ++i) {
            float len   = *len_it++;
            doc_lens[i] = len;
            lens_sum += len;
        }

        float avg_len = float(lens_sum / double(num_docs));

        typename block_wand_type::builder builder(coll, params);
        {
            pisa::progress progress("Processing posting lists", coll.size());
            for (auto const &seq : coll) {
                uint32_t term_count = std::accumulate(seq.freqs.begin(), seq.freqs.end(), 0);
                collection_terms_count.push_back(term_count);
                auto v = builder.add_sequence(seq, coll, doc_lens, avg_len, block_size);
                max_term_weight.push_back(v);
                progress.update(1);
            }
        }
        builder.build(m_block_wand);
        m_doc_lens.steal(doc_lens);
        m_collection_terms_count.steal(collection_terms_count);
        m_max_term_weight.steal(max_term_weight);
        m_lens_sum = lens_sum;
        m_avg_len  = avg_len;
    }

    double collection_len() const { return m_lens_sum; }

    float doc_len(uint64_t doc_id) const { return m_doc_lens[doc_id]; }

    uint32_t collection_term_frequency(uint64_t term_id) const {
        return m_collection_terms_count[term_id];
    }

    float max_term_weight(uint64_t list) const { return m_max_term_weight[list]; }

    wand_data_enumerator getenum(size_t i) const { return m_block_wand.get_enum(i); }

    const block_wand_type &get_block_wand() const { return m_block_wand; }

    template <typename Visitor>
    void map(Visitor &visit) {
        visit(m_block_wand, "m_block_wand")(m_collection_terms_count, "m_collection_terms_count")(
            m_doc_lens, "m_doc_lens")(m_max_term_weight, "m_max_term_weight")(
            m_lens_sum, "m_lens_sum")(m_avg_len, "m_avg_len");
    }

   private:
    block_wand_type                   m_block_wand;
    mapper::mappable_vector<uint32_t> m_collection_terms_count;
    mapper::mappable_vector<float>    m_doc_lens;
    mapper::mappable_vector<float>    m_max_term_weight;
    double                            m_lens_sum;
    float avg_len m_avg_len;
};
}  // namespace pisa
