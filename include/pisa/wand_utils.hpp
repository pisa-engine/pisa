#pragma once

#include "boost/variant.hpp"

#include "binary_freq_collection.hpp"
#include "configuration.hpp"
#include "score_opt_partition.hpp"

namespace pisa {

struct FixedBlock {
    uint64_t size;
    FixedBlock() : size(configuration::get().block_size) {}
    FixedBlock(const uint64_t in_size) : size(in_size) {}
};

struct VariableBlock {
    float lambda;
    VariableBlock() : lambda(configuration::get().fixed_cost_wand_partition) {}
    VariableBlock(const float in_lambda) : lambda(in_lambda) {}
};

using BlockSize = boost::variant<FixedBlock, VariableBlock>;

template <typename Scorer = bm25>
std::pair<std::vector<uint32_t>, std::vector<float>> static_block_partition(
    binary_freq_collection::sequence const &seq,
    std::vector<float> const &doc_lens,
    float avg_len,
    const uint64_t block_size)
{
    std::vector<uint32_t> block_docid;
    std::vector<float> block_max_term_weight;

    // Auxiliary vector
    float max_score = 0;
    float block_max_score = 0;
    size_t current_block = 0;
    size_t i;

    for (i = 0; i < seq.docs.size(); ++i) {
        uint64_t docid = *(seq.docs.begin() + i);
        uint64_t freq = *(seq.freqs.begin() + i);
        float score = Scorer::doc_term_weight(freq, doc_lens[docid] / avg_len);
        max_score = std::max(max_score, score);
        if (i == 0 || (i / block_size) == current_block) {
            block_max_score = std::max(block_max_score, score);
        } else {
            block_docid.push_back(*(seq.docs.begin() + i) - 1);
            block_max_term_weight.push_back(block_max_score);
            current_block++;
            block_max_score = std::max((float)0, score);
        }
    }
    block_docid.push_back(*(seq.docs.begin() + seq.docs.size() - 1));
    block_max_term_weight.push_back(block_max_score);

    return std::make_pair(block_docid, block_max_term_weight);
}

template <typename Scorer = bm25>
std::pair<std::vector<uint32_t>, std::vector<float>> variable_block_partition(
    binary_freq_collection const &coll,
    binary_freq_collection::sequence const &seq,
    std::vector<float> const &doc_lens,
    float avg_len,
    const float lambda)
{

    auto eps1 = configuration::get().eps1_wand;
    auto eps2 = configuration::get().eps2_wand;

    // Auxiliary vector
    using doc_score_t = std::pair<uint64_t, float>;
    std::vector<doc_score_t> doc_score;

    std::transform(seq.docs.begin(),
                   seq.docs.end(),
                   seq.freqs.begin(),
                   std::back_inserter(doc_score),
                   [&](const uint64_t &doc, const uint64_t &freq) -> doc_score_t {
                       return {doc, Scorer::doc_term_weight(freq, doc_lens[doc] / avg_len)};
                   });

    float estimated_idf = Scorer::query_term_weight(1, seq.docs.size(), coll.num_docs());
    auto p = score_opt_partition(
        doc_score.begin(), 0, doc_score.size(), eps1, eps2, lambda, estimated_idf);

    return std::make_pair(p.docids, p.max_values);
}

} // namespace pisa
