#pragma once

#include <algorithm>
#include <numeric>
#include <unordered_set>

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "binary_collection.hpp"
#include "binary_freq_collection.hpp"
#include "mappable/mappable_vector.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_range.hpp"
#include "wand_data_raw.hpp"

#include "linear_quantizer.hpp"
#include "scorer/scorer.hpp"

class enumerator;
namespace pisa {

template <typename block_wand_type = wand_data_raw>
class wand_data {
  public:
    using wand_data_enumerator = typename block_wand_type::enumerator;

    wand_data() = default;
    explicit wand_data(MemorySource source) : m_source(std::move(source))
    {
        mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
    }

    template <typename LengthsIterator>
    wand_data(
        LengthsIterator len_it,
        uint64_t num_docs,
        binary_freq_collection const& coll,
        const ScorerParams& scorer_params,
        BlockSize block_size,
        bool is_quantized,
        std::unordered_set<size_t> const& terms_to_drop)
        : m_num_docs(num_docs)
    {
        std::vector<uint32_t> doc_lens(num_docs);
        std::vector<float> max_term_weight;
        std::vector<uint32_t> term_occurrence_counts;
        std::vector<uint32_t> term_posting_counts;
        global_parameters params;
        spdlog::info("Reading sizes...");

        for (size_t i = 0; i < num_docs; ++i) {
            uint32_t len = *len_it++;
            doc_lens[i] = len;
            m_collection_len += len;
        }

        m_avg_len = float(m_collection_len / double(num_docs));

        typename block_wand_type::builder builder(coll, params);

        {
            pisa::progress progress("Storing terms statistics", coll.size());
            size_t term_id = 0;
            for (auto const& seq: coll) {
                if (terms_to_drop.find(term_id) != terms_to_drop.end()) {
                    progress.update(1);
                    term_id += 1;
                    continue;
                }

                size_t term_occurrence_count = std::accumulate(seq.freqs.begin(), seq.freqs.end(), 0);
                term_occurrence_counts.push_back(term_occurrence_count);
                term_posting_counts.push_back(seq.docs.size());
                term_id += 1;
                progress.update(1);
            }
        }
        m_doc_lens.steal(doc_lens);
        m_term_occurrence_counts.steal(term_occurrence_counts);
        m_term_posting_counts.steal(term_posting_counts);

        auto scorer = scorer::from_params(scorer_params, *this);
        {
            pisa::progress progress("Storing score upper bounds", coll.size());
            size_t term_id = 0;
            size_t new_term_id = 0;
            for (auto const& seq: coll) {
                if (terms_to_drop.find(term_id) != terms_to_drop.end()) {
                    progress.update(1);
                    term_id += 1;
                    continue;
                }
                auto v = builder.add_sequence(
                    seq, coll, doc_lens, m_avg_len, scorer->term_scorer(new_term_id), block_size);
                max_term_weight.push_back(v);
                m_index_max_term_weight = std::max(m_index_max_term_weight, v);
                term_id += 1;
                new_term_id += 1;
                progress.update(1);
            }
            if (is_quantized) {
                LinearQuantizer quantizer(
                    m_index_max_term_weight, configuration::get().quantization_bits);
                for (auto&& w: max_term_weight) {
                    w = quantizer(w);
                }
                builder.quantize_block_max_term_weights(m_index_max_term_weight);
            }
        }
        builder.build(m_block_wand);
        m_max_term_weight.steal(max_term_weight);
    }

    float norm_len(uint64_t doc_id) const { return m_doc_lens[doc_id] / m_avg_len; }

    size_t doc_len(uint64_t doc_id) const { return m_doc_lens[doc_id]; }

    size_t term_occurrence_count(uint64_t term_id) const
    {
        return m_term_occurrence_counts[term_id];
    }

    size_t term_posting_count(uint64_t term_id) const { return m_term_posting_counts[term_id]; }

    float index_max_term_weight() const { return m_index_max_term_weight; }

    size_t num_docs() const { return m_num_docs; }

    float avg_len() const { return m_avg_len; }

    uint64_t collection_len() const { return m_collection_len; }

    float max_term_weight(uint64_t list) const { return m_max_term_weight[list]; }

    wand_data_enumerator getenum(size_t i) const
    {
        return m_block_wand.get_enum(i, index_max_term_weight());
    }

    const block_wand_type& get_block_wand() const { return m_block_wand; }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_block_wand, "m_block_wand")(m_doc_lens, "m_doc_lens")(

            m_term_occurrence_counts, "m_term_occurrence_counts")(
            m_term_posting_counts, "m_term_posting_counts")(m_avg_len, "m_avg_len")(
            m_collection_len, "m_collection_len")(m_num_docs, "m_num_docs")(
            m_max_term_weight, "m_max_term_weight")(
            m_index_max_term_weight, "m_index_max_term_weight");
    }

  private:
    uint64_t m_num_docs = 0;
    float m_avg_len = 0;
    uint64_t m_collection_len = 0;
    float m_index_max_term_weight = 0;
    block_wand_type m_block_wand;
    mapper::mappable_vector<uint32_t> m_doc_lens;
    mapper::mappable_vector<uint32_t> m_term_occurrence_counts;
    mapper::mappable_vector<uint32_t> m_term_posting_counts;
    mapper::mappable_vector<float> m_max_term_weight;
    MemorySource m_source;
};

inline void create_wand_data(
    std::string const& output,
    std::string const& input_basename,
    BlockSize block_size,
    const ScorerParams& scorer_params,
    bool range,
    bool compress,
    bool quantize,
    std::unordered_set<size_t> const& dropped_term_ids)
{
    spdlog::info("Dropping {} terms", dropped_term_ids.size());
    binary_collection sizes_coll((input_basename + ".sizes").c_str());
    binary_freq_collection coll(input_basename.c_str());

    if (compress) {
        wand_data<wand_data_compressed<>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_params,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output.c_str());
    } else if (range) {
        wand_data<wand_data_range<128, 1024>> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_params,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output.c_str());
    } else {
        wand_data<wand_data_raw> wdata(
            sizes_coll.begin()->begin(),
            coll.num_docs(),
            coll,
            scorer_params,
            block_size,
            quantize,
            dropped_term_ids);
        mapper::freeze(wdata, output.c_str());
    }
}

}  // namespace pisa
