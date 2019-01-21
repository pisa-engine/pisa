#pragma once

#include "spdlog/spdlog.h"

#include "succinct/mappable_vector.hpp"

#include "binary_freq_collection.hpp"
#include "scorer/bm25.hpp"
#include "util/util.hpp"
#include "wand_utils.hpp"

namespace pisa {

    template<typename Scorer = bm25>
    class wand_data_raw {
    public:
        wand_data_raw() { }

        class builder{
           public:
            builder(partition_type                type,
                    binary_freq_collection const &coll,
                    global_parameters const &     params)
            {
                (void) coll;
                (void) params;
                this->type = type;
                spdlog::info("Storing max weight for each list and for each block...");
                total_elements = 0;
                total_blocks = 0;
                effective_list = 0;
                blocks_start.push_back(0);
            }

            float add_sequence(binary_freq_collection::sequence const &seq, binary_freq_collection const &coll, std::vector<float> const & norm_lens){

                if (seq.docs.size() > configuration::get().threshold_wand_list) {

                    auto t = ((type == partition_type::fixed_blocks) ? static_block_partition(seq, norm_lens)
                                                      : variable_block_partition(coll, seq, norm_lens));

                    block_max_term_weight.insert(block_max_term_weight.end(), t.second.begin(),
                                                 t.second.end());
                    block_docid.insert(block_docid.end(), t.first.begin(), t.first.end());
                    max_term_weight.push_back(*(std::max_element(t.second.begin(), t.second.end())));
                    blocks_start.push_back(t.first.size() + blocks_start.back());

                    total_elements += seq.docs.size();
                    total_blocks += t.first.size();
                    effective_list++;
                } else {
                    max_term_weight.push_back(0.0f);
                    blocks_start.push_back(blocks_start.back());
                }

                return max_term_weight.back();

            }

            void build(wand_data_raw &wdata) {
                wdata.m_block_max_term_weight.steal(block_max_term_weight);
                wdata.m_blocks_start.steal(blocks_start);
                wdata.m_block_docid.steal(block_docid);
                spdlog::info("number of elements / number of blocks: {}",
                             static_cast<float>(total_elements) / static_cast<float>(total_blocks));
            }

            partition_type type;
            uint64_t total_elements;
            uint64_t total_blocks;
            uint64_t effective_list;
            std::vector<float> max_term_weight;
            std::vector<uint64_t> blocks_start;
            std::vector<float> block_max_term_weight;
            std::vector<uint32_t> block_docid;
        };
        class enumerator{
            friend class wand_data_raw;
        public:

            enumerator(uint32_t _block_start, uint32_t _block_number, mapper::mappable_vector<float> const & max_term_weight,
            mapper::mappable_vector<uint32_t> const & block_docid) :
                    cur_pos(0),
                    block_start(_block_start),
                    block_number(_block_number),
                    m_block_max_term_weight(max_term_weight),
                    m_block_docid(block_docid)
                        {}


            void DS2I_NOINLINE next_geq(uint64_t lower_bound) {
                while (cur_pos + 1 < block_number &&
                        m_block_docid[block_start + cur_pos] <
                       lower_bound) {
                    cur_pos++;
                }
            }


            float DS2I_FLATTEN_FUNC score() const {
                return m_block_max_term_weight[block_start + cur_pos];
            }

            uint64_t DS2I_FLATTEN_FUNC docid() const {
                return m_block_docid[block_start + cur_pos];
            }


            uint64_t DS2I_FLATTEN_FUNC find_next_skip() {
                return m_block_docid[cur_pos + block_start];
            }

        private:
            uint64_t cur_pos;
            uint64_t block_start;
            uint64_t block_number;
            mapper::mappable_vector<float> const &m_block_max_term_weight;
            mapper::mappable_vector<uint32_t> const &m_block_docid;

        };

        enumerator get_enum(uint32_t i) const {
            return enumerator(m_blocks_start[i], m_blocks_start[i+1] - m_blocks_start[i], m_block_max_term_weight, m_block_docid);
        }

        template<typename Visitor>
        void map(Visitor &visit) {
            visit
                    (m_blocks_start, "m_blocks_start")
                    (m_block_max_term_weight, "m_block_max_term_weight")
                    (m_block_docid, "m_block_docid");
        }

    private:

        mapper::mappable_vector<uint64_t> m_blocks_start;
        mapper::mappable_vector<float> m_block_max_term_weight;
        mapper::mappable_vector<uint32_t> m_block_docid;

    };

}
