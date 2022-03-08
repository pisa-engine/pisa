#pragma once

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mappable_vector.hpp"

#include "binary_freq_collection.hpp"
#include "global_parameters.hpp"
#include "linear_quantizer.hpp"
#include "util/compiler_attribute.hpp"
#include "wand_utils.hpp"

namespace pisa {

class wand_data_raw {
  public:
    wand_data_raw() = default;

    class builder {
      public:
        builder(binary_freq_collection const& coll, global_parameters const& params)
        {
            (void)coll;
            (void)params;
            spdlog::info("Storing max weight for each list and for each block...");
            total_elements = 0;
            total_blocks = 0;
            effective_list = 0;
            blocks_start.push_back(0);
        }

        template <typename Scorer>
        float add_sequence(
            binary_freq_collection::sequence const& seq,
            binary_freq_collection const& coll,
            [[maybe_unused]] std::vector<uint32_t> const& doc_lens,
            float avg_len,
            Scorer scorer,
            BlockSize block_size)
        {
            auto t = block_size.type() == typeid(FixedBlock)
                ? static_block_partition(seq, scorer, boost::get<FixedBlock>(block_size).size)
                : variable_block_partition(
                    coll, seq, scorer, boost::get<VariableBlock>(block_size).lambda);

            block_max_term_weight.insert(
                block_max_term_weight.end(), t.second.begin(), t.second.end());
            block_docid.insert(block_docid.end(), t.first.begin(), t.first.end());
            max_term_weight.push_back(*(std::max_element(t.second.begin(), t.second.end())));
            blocks_start.push_back(t.first.size() + blocks_start.back());

            total_elements += seq.docs.size();
            total_blocks += t.first.size();
            effective_list++;
            return max_term_weight.back();
        }

        void quantize_block_max_term_weights(float index_max_term_weight)
        {
            LinearQuantizer quantizer(index_max_term_weight, configuration::get().quantization_bits);
            for (auto&& w: block_max_term_weight) {
                w = quantizer(w);
            }
        }

        void build(wand_data_raw& wdata)
        {
            wdata.m_block_max_term_weight.steal(block_max_term_weight);
            wdata.m_blocks_start.steal(blocks_start);
            wdata.m_block_docid.steal(block_docid);
            spdlog::info(
                "number of elements / number of blocks: {}",
                static_cast<float>(total_elements) / static_cast<float>(total_blocks));
        }

        uint64_t total_elements;
        uint64_t total_blocks;
        uint64_t effective_list;
        std::vector<float> max_term_weight;
        std::vector<uint64_t> blocks_start;
        std::vector<float> block_max_term_weight;
        std::vector<uint32_t> block_docid;
    };
    class enumerator {
        friend class wand_data_raw;

      public:
        enumerator(
            uint32_t _block_start,
            uint32_t _block_number,
            mapper::mappable_vector<float> const& max_term_weight,
            mapper::mappable_vector<uint32_t> const& block_docid)
            : cur_pos(0),
              block_start(_block_start),
              block_number(_block_number),
              m_block_max_term_weight(max_term_weight),
              m_block_docid(block_docid)
        {}

        void PISA_NOINLINE next_geq(uint64_t lower_bound)
        {
            while (cur_pos + 1 < block_number && m_block_docid[block_start + cur_pos] < lower_bound) {
                cur_pos++;
            }
        }

        float PISA_FLATTEN_FUNC score() const
        {
            return m_block_max_term_weight[block_start + cur_pos];
        }

        uint64_t PISA_FLATTEN_FUNC docid() const { return m_block_docid[block_start + cur_pos]; }

        uint64_t PISA_FLATTEN_FUNC find_next_skip() { return m_block_docid[cur_pos + block_start]; }

      private:
        uint64_t cur_pos;
        uint64_t block_start;
        uint64_t block_number;
        mapper::mappable_vector<float> const& m_block_max_term_weight;
        mapper::mappable_vector<uint32_t> const& m_block_docid;
    };

    enumerator get_enum(uint32_t i, float) const
    {
        return enumerator(
            m_blocks_start[i],
            m_blocks_start[i + 1] - m_blocks_start[i],
            m_block_max_term_weight,
            m_block_docid);
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_blocks_start, "m_blocks_start")(m_block_max_term_weight, "m_block_max_term_weight")(
            m_block_docid, "m_block_docid");
    }

  private:
    mapper::mappable_vector<uint64_t> m_blocks_start;
    mapper::mappable_vector<float> m_block_max_term_weight;
    mapper::mappable_vector<uint32_t> m_block_docid;
};

}  // namespace pisa
