#pragma once

#include "configuration.hpp"
#include "score_partitioning.hpp"
#include "global_parameters.hpp"

namespace ds2i {

enum class partition_type { fixed_blocks, variable_blocks };

template <typename Scorer = bm25>
std::tuple<std::vector<uint32_t>, std::vector<uint32_t>, std::vector<float>>
static_block_partition(binary_freq_collection::sequence const &seq,
                       std::vector<float> const &norm_lens) {
  std::vector<uint32_t> sizes;
  std::vector<uint32_t> block_docid;
  std::vector<float> block_max_term_weight;
  uint64_t block_size = configuration::get().block_size;

  // Auxiliary vector
  float max_score = 0;
  float block_max_score = 0;
  size_t current_block = 0;
  size_t i;

  for (i = 0; i < seq.docs.size(); ++i) {
    uint64_t docid = *(seq.docs.begin() + i);
    uint64_t freq = *(seq.freqs.begin() + i);
    float score = Scorer::doc_term_weight(freq, norm_lens[docid]);
    max_score = std::max(max_score, score);
    if (i == 0 || (i / block_size) == current_block) {
      block_max_score = std::max(block_max_score, score);
    } else {
      block_docid.push_back(*(seq.docs.begin() + i) - 1);
      block_max_term_weight.push_back(block_max_score);
      current_block++;
      block_max_score = std::max((float)0, score);
      sizes.push_back(block_size);
    }
  }
  block_docid.push_back(*(seq.docs.begin() + seq.docs.size() - 1));
  block_max_term_weight.push_back(block_max_score);
  sizes.push_back((i % block_size) ? i % block_size : block_size);

  return std::make_tuple(sizes, block_docid, block_max_term_weight);
}

template <typename Scorer = bm25>
std::tuple<std::vector<uint32_t>, std::vector<uint32_t>, std::vector<float>>
variable_block_partition(binary_freq_collection const &coll,
                         binary_freq_collection::sequence const &seq,
                         std::vector<float> const &norm_lens) {

  auto eps1 = configuration::get().eps1_wand;
  auto eps2 = configuration::get().eps2_wand;
  auto fixed_cost = configuration::get().fixed_cost_wand_partition;

  // Auxiliary vector
  std::vector<std::tuple<uint64_t, float, bool>> doc_score_top;
  float max_score = 0;

  for (size_t i = 0; i < seq.docs.size(); ++i) {
    uint64_t docid = *(seq.docs.begin() + i);
    uint64_t freq = *(seq.freqs.begin() + i);
    float score = Scorer::doc_term_weight(freq, norm_lens[docid]);
    doc_score_top.emplace_back(docid, score, false);
    max_score = std::max(max_score, score);
  }

  float estimated_idf =
      Scorer::query_term_weight(1, seq.docs.size(), coll.num_docs());
  auto p = score_opt_partition(doc_score_top.begin(), 0, doc_score_top.size(),
                               eps1, eps2, fixed_cost, estimated_idf);

  return std::make_tuple(p.sizes, p.docids, p.max_values);
}

}  // namespace ds2i
