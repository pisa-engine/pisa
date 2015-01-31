#pragma once

#include <succinct/mappable_vector.hpp>

#include "binary_freq_collection.hpp"
#include "bm25.hpp"
#include "util.hpp"

namespace ds2i {

    template <typename Scorer = bm25>
    class wand_data {
    public:
        wand_data()
        {}

        template <typename LengthsIterator>
        wand_data(LengthsIterator len_it, uint64_t num_docs,
                  binary_freq_collection const& coll)
        {
            std::vector<float> norm_lens(num_docs);
            double lens_sum = 0;
            logger() << "Reading sizes..." << std::endl;
            for (size_t i = 0; i < num_docs; ++i) {
                float len = *len_it++;
                norm_lens[i] = len;
                lens_sum += len;
            }
            float avg_len = float(lens_sum / double(num_docs));
            for (size_t i = 0; i < num_docs; ++i) {
                norm_lens[i] /= avg_len;
            }

            logger() << "Storing max weight for each list..." << std::endl;
            std::vector<float> max_term_weight;
            for (auto const& seq: coll) {
                float max_score = 0;
                for (size_t i = 0; i < seq.docs.size(); ++i) {
                    uint64_t docid = *(seq.docs.begin() + i);
                    uint64_t freq = *(seq.freqs.begin() + i);
                    float score = Scorer::doc_term_weight(freq, norm_lens[docid]);
                    max_score = std::max(max_score, score);
                }
                max_term_weight.push_back(max_score);
                if ((max_term_weight.size() % 1000000) == 0) {
                    logger() << max_term_weight.size() << " list processed" << std::endl;
                }
            }
            logger() << max_term_weight.size() << " list processed" << std::endl;

            m_norm_lens.steal(norm_lens);
            m_max_term_weight.steal(max_term_weight);
        }

        float norm_len(uint64_t doc_id) const
        {
            return m_norm_lens[doc_id];
        }

        float max_term_weight(uint64_t term_id) const
        {
            return m_max_term_weight[term_id];
        }

        void swap(wand_data& other)
        {
            m_norm_lens.swap(other.m_norm_lens);
            m_max_term_weight.swap(other.m_max_term_weight);
        }

        template <typename Visitor>
        void map(Visitor& visit)
        {
            visit
                (m_norm_lens, "m_norm_lens")
                (m_max_term_weight, "m_max_term_weight")
                ;
        }

    private:
        succinct::mapper::mappable_vector<float> m_norm_lens;
        succinct::mapper::mappable_vector<float> m_max_term_weight;
    };

}
