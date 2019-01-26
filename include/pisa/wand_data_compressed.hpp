#pragma once

#include "spdlog/spdlog.h"

#include "binary_freq_collection.hpp"
#include "bitvector_collection.hpp"
#include "configuration.hpp"
#include "score_opt_partition.hpp"
#include "scorer/bm25.hpp"
#include "util/util.hpp"

#include "sequence/positive_sequence.hpp"
#include "util/index_build_utils.hpp"
#include "wand_utils.hpp"
#include "global_parameters.hpp"


namespace pisa {
namespace {
    static const size_t score_bits_size = broadword::msb(configuration::get().reference_size);
}

    class uniform_score_compressor{

    public:
        class builder{
        public:
            builder(uint64_t num_docs, global_parameters const& params)
                : m_params(params)
                , m_num_docs((num_docs+1) << score_bits_size)
                , m_docs_sequences(params)
            {}

            std::vector<uint32_t> compress_data(std::vector<float> effective_scores){
                float quant = 1.f/configuration::get().reference_size;

                //Partition scores.
                std::vector<uint32_t> score_indexes;
                score_indexes.reserve(effective_scores.size());
                for(const auto& score: effective_scores) {
                    size_t pos = 1;
                    while (score > quant*pos)
                        pos++;
                    score_indexes.push_back(pos-1);

                }
                return score_indexes;
            }

            template <typename Sequence = compact_elias_fano, typename DocsIterator>
            void add_posting_list(uint64_t n, DocsIterator docs_begin, DocsIterator score_begin)
            {
                std::vector<uint64_t> temp;
                for(size_t i =0; i < n; ++i)
                {
                    uint64_t elem = *(docs_begin + i);
                    elem = elem << score_bits_size;
                    elem += *(score_begin + i);
                    temp.push_back(elem);
                }

                if (!n) throw std::invalid_argument("List must be nonempty");
                bit_vector_builder docs_bits;
                write_gamma_nonzero(docs_bits, n);
                Sequence::write(docs_bits, temp.begin(),
                                m_num_docs, n,
                                m_params);
                m_docs_sequences.append(docs_bits);
            }

            void build(bitvector_collection& docs_sequences){
                m_docs_sequences.build(docs_sequences);
            }

        global_parameters params() {
            return m_params;
        }

        uint64_t num_docs() {
            return m_num_docs;
        }

        private:
            global_parameters m_params;
            uint64_t m_num_docs;
            bitvector_collection::builder m_docs_sequences;


        };

        static float inline score(uint32_t index){
                const float quant = 1.f/configuration::get().reference_size;
                return quant * (index + 1);
        }

    };

    template<typename Scorer = bm25, typename score_compressor = uniform_score_compressor>
    class wand_data_compressed {
    public:
        class builder{
           public:
            builder(partition_type                type,
                    binary_freq_collection const &coll,
                    global_parameters const &     params)
                : total_elements(0),
                  total_blocks(0),
                  type(type),
                  params(params),
                  compressor_builder(coll.num_docs(), params) {
                spdlog::info("Storing max weight for each list and for each block...");
            }

            float add_sequence(binary_freq_collection::sequence const &seq, binary_freq_collection const &coll, std::vector<float> const & norm_lens){

                if (seq.docs.size() > configuration::get().threshold_wand_list) {

                    auto t = ((type == partition_type::fixed_blocks) ? static_block_partition(seq, norm_lens)
                                                      : variable_block_partition(coll, seq, norm_lens));

                    auto ind = compressor_builder.compress_data(t.second);

                    compressor_builder.add_posting_list(t.first.size(), t.first.begin(),
                                                           ind.begin());

                    max_term_weight.push_back(*(std::max_element(t.second.begin(), t.second.end())));
                    total_elements += seq.docs.size();
                    total_blocks += t.first.size();
                } else {
                    max_term_weight.push_back(0.0f);
                    std::vector<uint32_t> temp = {0};
                    compressor_builder.add_posting_list(temp.size(), temp.begin(),
                                                           temp.begin());
                }

                return max_term_weight.back();

            }

            void build(wand_data_compressed &wdata) {
                wdata.m_num_docs = compressor_builder.num_docs();
                wdata.m_params = compressor_builder.params();
                compressor_builder.build(wdata.m_docs_sequences);
                spdlog::info("number of elements / number of blocks: {}",
                             (float)total_elements / (float)total_blocks);
            }

            uint64_t total_elements;
            uint64_t total_blocks;
            partition_type type;
            std::vector<float> score_references;
            std::vector<float> max_term_weight;
            global_parameters const &params;
            typename score_compressor::builder compressor_builder;
        };

        class enumerator{
            friend class wand_data_compressed;
        public:
            enumerator(compact_elias_fano::enumerator docs_enum)
                : m_docs_enum(docs_enum)
            {
                reset();
            }

            void reset()
            {
                uint64_t val = m_docs_enum.move(0).second;
                m_cur_docid = val >> score_bits_size;
                uint64_t mask = configuration::get().reference_size - 1;
		m_cur_score_index = (val & mask);
            }

            void DS2I_FLATTEN_FUNC next_geq(uint64_t lower_bound) {
                if(docid() != lower_bound) {
                    lower_bound = lower_bound << score_bits_size;
                    auto val = m_docs_enum.next_geq(lower_bound);
                    m_cur_docid = val.second >> score_bits_size;
		    uint64_t mask = configuration::get().reference_size - 1;
                    m_cur_score_index = (val.second & mask);
                }
            }

            float DS2I_FLATTEN_FUNC score()  {
                return score_compressor::score(m_cur_score_index);
            }

            uint64_t DS2I_FLATTEN_FUNC docid() const {
                return m_cur_docid;
            }

        private:
            uint64_t m_cur_docid;
            uint64_t m_cur_score_index;
            compact_elias_fano::enumerator m_docs_enum;
        };

        uint64_t size() const
        {
            return m_docs_sequences.size();
        }

        uint64_t num_docs() const
        {
            return m_num_docs;
        }

        enumerator get_enum(size_t i) const
        {
            assert(i < size());
            auto docs_it = m_docs_sequences.get(m_params, i);

            uint64_t n = read_gamma_nonzero(docs_it);
            typename compact_elias_fano::enumerator docs_enum(m_docs_sequences.bits(),
                                                        docs_it.position(),
                                                        num_docs(), n,
                                                        m_params);

            return enumerator(docs_enum);
        }


        template<typename Visitor>
        void map(Visitor& visit)
        {
            visit
                (m_params, "m_params")
                (m_num_docs, "m_num_docs")
                (m_docs_sequences, "m_docs_sequences");
        }

    private:
        global_parameters m_params;
        uint64_t m_num_docs;
        bitvector_collection m_docs_sequences;
    };

}
