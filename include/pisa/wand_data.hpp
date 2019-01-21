#pragma once

#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "scorer/bm25.hpp"
#include "succinct/mappable_vector.hpp"
#include "util/util.hpp"
#include "wand_data_raw.hpp"

class enumerator;
namespace pisa {

    template<typename Scorer=bm25, typename block_wand_type=wand_data_raw<bm25> >
    class wand_data {
    public:

        using wand_data_enumerator = typename block_wand_type::enumerator;

        wand_data() { }

        template<typename LengthsIterator>
        wand_data(LengthsIterator len_it, uint64_t num_docs,
                      binary_freq_collection const &coll, partition_type type = partition_type::fixed_blocks) {
            std::vector<float> norm_lens(num_docs);
            std::vector<float> max_term_weight;
            global_parameters params;
            double lens_sum = 0;
            spdlog::info("Reading sizes...");

            for (size_t i = 0; i < num_docs; ++i) {
                float len = *len_it++;
                norm_lens[i] = len;
                lens_sum += len;
            }

            float avg_len = float(lens_sum / double(num_docs));
            for(auto& norm_len: norm_lens) {
                norm_len /= avg_len;
            }

            typename block_wand_type::builder builder(type, coll, params);

            for (auto const &seq: coll) {
                auto v = builder.add_sequence(seq, coll, norm_lens);
                max_term_weight.push_back(v);
                if ((max_term_weight.size() % 1000000) == 0) {
                    spdlog::info("{} lists processed", max_term_weight.size());
                }
            }
            if ((max_term_weight.size() % 1000000) != 0) {
                spdlog::info("{} lists processed", max_term_weight.size());
            }

            builder.build(m_block_wand);
            m_norm_lens.steal(norm_lens);
            m_max_term_weight.steal(max_term_weight);

        }

        float norm_len(uint64_t doc_id) const {
            return m_norm_lens[doc_id];
        }

        float max_term_weight(uint64_t list) const {
            return m_max_term_weight[list];
        }


        wand_data_enumerator getenum(size_t i) const {
            return m_block_wand.get_enum(i);
        }

        template<typename Visitor>
        void map(Visitor &visit) {
            visit
                    (m_block_wand, "m_block_wand")
                    (m_norm_lens, "m_norm_lens")
                    (m_max_term_weight, "m_max_term_weight");
        }


    private:
        block_wand_type m_block_wand;
        mapper::mappable_vector<float> m_norm_lens;
        mapper::mappable_vector<float> m_max_term_weight;
    };
}
