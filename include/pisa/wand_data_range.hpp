#pragma once

#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "global_parameters.hpp"
#include "linear_quantizer.hpp"
#include "mappable/mapper.hpp"
#include "util/compiler_attribute.hpp"
#include "util/util.hpp"
#include "wand_utils.hpp"

namespace pisa {

template <size_t range_size = 128, size_t min_list_lenght = 1024>
class wand_data_range {
  public:
    template <typename List, typename Fn>
    void for_each_posting(List& list, Fn func) const
    {
        while (list.position() < list.size()) {
            func(list.docid(), list.freq());
            list.next();
        }
    }

    template <typename List, typename Fn>
    auto compute_block_max_scores(List& list, Fn scorer) const
    {
        std::vector<float> block_max_scores(m_blocks_num, 0.0F);
        for_each_posting(list, [&](auto docid, auto freq) {
            float& current_max = block_max_scores[docid / range_size];
            current_max = std::max(current_max, scorer(docid, freq));
        });
        return block_max_scores;
    };

    class builder {
      public:
        builder(binary_freq_collection const& coll, [[maybe_unused]] global_parameters const& params)
            : blocks_num(ceil_div(coll.num_docs(), range_size)),
              total_elements(0),
              blocks_start{0},
              block_max_term_weight{}
        {
            auto posting_lists = std::distance(coll.begin(), coll.end());
            spdlog::info("Storing max weight for each list and for each block...");
            spdlog::info(
                "Range size: {}. Number of docs: {}."
                "Blocks per posting list: {}. Posting lists: {}.",
                range_size,
                coll.num_docs(),
                blocks_num,
                posting_lists);
        }

        template <typename Scorer>
        float add_sequence(
            binary_freq_collection::sequence const& term_seq,
            [[maybe_unused]] binary_freq_collection const& coll,
            [[maybe_unused]] std::vector<uint32_t> const& doc_lens,
            float avg_len,
            Scorer scorer,
            [[maybe_unused]] BlockSize block_size)
        {
            float max_score = 0.0F;

            std::vector<float> b_max(blocks_num, 0.0F);
            for (auto i = 0; i < term_seq.docs.size(); ++i) {
                uint64_t docid = *(term_seq.docs.begin() + i);
                uint64_t freq = *(term_seq.freqs.begin() + i);
                float score = scorer(docid, freq);
                max_score = std::max(max_score, score);
                size_t pos = docid / range_size;
                float& bm = b_max[pos];
                bm = std::max(bm, score);
            }
            if (term_seq.docs.size() >= min_list_lenght) {
                block_max_term_weight.insert(block_max_term_weight.end(), b_max.begin(), b_max.end());
                blocks_start.push_back(b_max.size() + blocks_start.back());
                total_elements += term_seq.docs.size();
            } else {
                blocks_start.push_back(blocks_start.back());
            }
            return max_score;
        }

        void quantize_block_max_term_weights(float index_max_term_weight)
        {
            LinearQuantizer quantizer(index_max_term_weight, configuration::get().quantization_bits);
            for (auto&& w: block_max_term_weight) {
                w = quantizer(w);
            }
        }

        void build(wand_data_range& wdata)
        {
            wdata.m_blocks_num = blocks_num;
            wdata.m_blocks_start.steal(blocks_start);
            wdata.m_block_max_term_weight.steal(block_max_term_weight);
            spdlog::info(
                "number of elements / number of blocks: {}",
                static_cast<float>(total_elements) / wdata.m_block_max_term_weight.size());
        }

        uint64_t blocks_num;
        uint64_t total_elements;
        std::vector<uint64_t> blocks_start;
        std::vector<float> block_max_term_weight;
    };

    class enumerator {
        friend class wand_data_range;

      public:
        enumerator(uint32_t _block_start, mapper::mappable_vector<float> const& block_max_term_weight)
            : cur_pos(0), block_start(_block_start), m_block_max_term_weight(block_max_term_weight)
        {}

        void PISA_NOINLINE next_block() { cur_pos += 1; }
        void PISA_NOINLINE next_geq(uint64_t lower_bound) { cur_pos = lower_bound / range_size; }
        uint64_t PISA_FLATTEN_FUNC docid() const { return (cur_pos + 1) * range_size; }

        float PISA_FLATTEN_FUNC score() const
        {
            return m_block_max_term_weight[block_start + cur_pos];
        }

      private:
        uint64_t cur_pos;
        uint64_t block_start;
        mapper::mappable_vector<float> const& m_block_max_term_weight;
    };

    enumerator get_enum(uint32_t i, float) const
    {
        return enumerator(m_blocks_start[i], m_block_max_term_weight);
    }

    static std::vector<bool> compute_live_blocks(
        std::vector<enumerator>& enums, float threshold, std::pair<uint32_t, uint32_t> document_range)
    {
        size_t len = ceil_div((document_range.second - document_range.first), range_size);
        std::vector<bool> live_blocks(len);
        for (auto&& e: enums) {
            e.next_geq(document_range.first);
        }
        for (size_t i = 0; i < len; ++i) {
            float score = 0.0F;
            for (auto&& e: enums) {
                score += e.score();
                e.next_block();
            }
            live_blocks[i] = (score > threshold);
        }
        return live_blocks;
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_blocks_num, "m_blocks_num")(m_blocks_start, "m_blocks_start")(
            m_block_max_term_weight, "m_block_max_term_weight");
    }

  private:
    uint64_t m_blocks_num{0};
    mapper::mappable_vector<uint64_t> m_blocks_start;
    mapper::mappable_vector<float> m_block_max_term_weight;
};

}  // namespace pisa
