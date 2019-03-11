#pragma once

#include "spdlog/spdlog.h"

#include "succinct/mappable_vector.hpp"

#include "binary_freq_collection.hpp"
#include "scorer/bm25.hpp"
#include "util/util.hpp"
#include "wand_utils.hpp"

namespace pisa {

template <size_t range_size = 128, size_t min_list_lenght = 1024, typename Scorer = bm25>
class wand_data_range {
   public:

    template<typename List>
    std::vector<float> compute_block_max_scores(List &list, std::function<float(uint64_t)> norm_lens) const {
        std::vector<float> block_max_scores(m_blocks_num, 0.0f);
        for (int i = 0; i < list.size(); ++i) {
            auto docid = list.docid();
            auto freq = list.freq();
            list.next();
            float score = Scorer::doc_term_weight(freq, norm_lens(docid));
            size_t pos = docid/range_size;
            float &bm = block_max_scores[pos];
            bm = std::max(bm, score);
        }
        return block_max_scores;
    }

    class builder {
       public:
        builder(partition_type type,
                binary_freq_collection const &coll,
                [[maybe_unused]] global_parameters const & params):
                blocks_num(ceil_div(coll.num_docs(), range_size)),
                type(type),
                total_elements(0),
                blocks_start{0},
                block_max_term_weight{} {
            (void)params;
            auto posting_lists = std::distance(coll.begin(), coll.end());
            spdlog::info("Storing max weight for each list and for each block...");
            spdlog::info("Range size: {}. Number of docs: {}. Blocks per posting list: {}. Posting lists: {}."
                , range_size, coll.num_docs(), blocks_num, posting_lists);   
        }

        float add_term_sequence(binary_freq_collection::sequence const &term_seq,
                                binary_freq_collection const &          coll,
                                std::vector<float> const &              norm_lens) {
            float max_score =0.0f;

            std::vector<float> b_max(blocks_num, 0.0f);
            for (auto i = 0; i < term_seq.docs.size(); ++i) {
                uint64_t docid = *(term_seq.docs.begin() + i);
                uint64_t freq = *(term_seq.freqs.begin() + i);
                float score = Scorer::doc_term_weight(freq, norm_lens[docid]);
                max_score = std::max(max_score, score);
                size_t pos = docid/range_size;
                float &bm = b_max[pos];
                bm = std::max(bm, score);
            }
            if (term_seq.docs.size() >= min_list_lenght) {
                block_max_term_weight.insert(
                    block_max_term_weight.end(), b_max.begin(), b_max.end());
                blocks_start.push_back(b_max.size() + blocks_start.back());
                total_elements += term_seq.docs.size();
            } else{
                blocks_start.push_back(blocks_start.back());
            }
            return max_score;
        }

        void build(wand_data_range &wdata) {
            wdata.m_blocks_num = blocks_num;
            wdata.m_blocks_start.steal(blocks_start);
            wdata.m_block_max_term_weight.steal(block_max_term_weight);
            spdlog::info("number of elements / number of blocks: {}",
                         static_cast<float>(total_elements) / wdata.m_block_max_term_weight.size());
        }

        uint64_t           blocks_num;
        partition_type     type;
        uint64_t           total_elements;
        std::vector<uint64_t> blocks_start;
        std::vector<float> block_max_term_weight;
    };

    class enumerator {
        friend class wand_data_range;

       public:
        enumerator(uint32_t                              _block_start,
                   mapper::mappable_vector<float> const &block_max_term_weight)
            : cur_pos(0),
              block_start(_block_start),
              m_block_max_term_weight(block_max_term_weight) {}

        void DS2I_NOINLINE next_block() {
            cur_pos += 1;
        }

        void DS2I_NOINLINE next_geq(uint64_t lower_bound) {
            cur_pos = lower_bound/range_size;
        }

        float DS2I_FLATTEN_FUNC score() const {
            return m_block_max_term_weight[block_start + cur_pos];
        }

        uint64_t DS2I_FLATTEN_FUNC docid() const { return (cur_pos+1) * range_size; }

       private:
        uint64_t                              cur_pos;
        uint64_t                              block_start;
        mapper::mappable_vector<float> const &m_block_max_term_weight;
    };

    enumerator get_enum(uint32_t i) const {
        return enumerator(
            m_blocks_start[i], m_block_max_term_weight);
    }

    static std::vector<bool> compute_live_blocks(std::vector<enumerator> &enums, 
                             float threshold, std::pair<int, int> document_range) {
        size_t len = ceil_div((document_range.second - document_range.first), range_size);
        std::vector<bool> live_blocks(len);
        for(auto&& e : enums) {
            e.next_geq(document_range.first);
        }
        for (size_t i = 0; i < len; ++i) {
            float score = 0.0f;
            for(auto&& e : enums) {
                score += e.score();
                e.next_block();
            }
            live_blocks[i] = (score > threshold);
        }
        return live_blocks;
    }

    template <typename Visitor>
    void map(Visitor &visit) {
        visit(m_blocks_num, "m_blocks_num")
             (m_blocks_start, "m_blocks_start")
             (m_block_max_term_weight, "m_block_max_term_weight");
    }

   private:
    uint64_t m_blocks_num;
    mapper::mappable_vector<uint64_t> m_blocks_start;
    mapper::mappable_vector<float> m_block_max_term_weight;
};

} // namespace pisa
