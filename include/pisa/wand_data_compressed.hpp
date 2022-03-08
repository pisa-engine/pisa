#pragma once

#include "boost/variant.hpp"
#include "spdlog/spdlog.h"
#include <range/v3/view/zip.hpp>

#include "binary_freq_collection.hpp"
#include "bitvector_collection.hpp"
#include "configuration.hpp"
#include "util/util.hpp"

#include "global_parameters.hpp"
#include "linear_quantizer.hpp"
#include "sequence/positive_sequence.hpp"
#include "util/index_build_utils.hpp"
#include "wand_utils.hpp"

namespace pisa {
namespace {
    static const size_t score_bits_size = configuration::get().quantization_bits;
}

class uniform_score_compressor {
  public:
    class builder {
      public:
        builder(uint64_t num_docs, global_parameters const& params)
            : m_params(params),
              m_num_docs((num_docs + 1) << score_bits_size),
              m_docs_sequences(params)
        {}

        std::vector<uint32_t> compress_data(std::vector<float> effective_scores, float max_score)
        {
            // Partition scores.
            LinearQuantizer quantizer(max_score, configuration::get().quantization_bits);
            std::vector<uint32_t> score_indexes;
            score_indexes.reserve(effective_scores.size());
            for (const auto& score: effective_scores) {
                score_indexes.push_back(quantizer(score) - 1);
            }
            return score_indexes;
        }

        template <typename Sequence = compact_elias_fano, typename DocsIterator>
        void add_posting_list(uint64_t n, DocsIterator docs_begin, DocsIterator score_begin)
        {
            std::vector<uint64_t> temp;
            for (size_t pos = 0; pos < n; ++pos) {
                uint64_t elem = *(docs_begin + pos);
                elem = elem << score_bits_size;
                elem += *(score_begin + pos);
                if (pos && elem < temp.back()) {
                    throw std::runtime_error(fmt::format(
                        "Sequence is not sorted: value at index {} "
                        "({}) lower than its predecessor ({})",
                        pos,
                        elem,
                        temp.back()));
                }
                temp.push_back(elem);
            }

            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            bit_vector_builder docs_bits;
            write_gamma_nonzero(docs_bits, n);
            Sequence::write(docs_bits, temp.begin(), m_num_docs, n, m_params);
            m_docs_sequences.append(docs_bits);
        }

        void build(bitvector_collection& docs_sequences) { m_docs_sequences.build(docs_sequences); }

        global_parameters params() { return m_params; }

        uint64_t num_docs() { return m_num_docs; }

      private:
        global_parameters m_params;
        uint64_t m_num_docs;
        bitvector_collection::builder m_docs_sequences;
    };

    static float inline score(uint32_t quantized_score)
    {
        const float quant = 1.F / (1U << configuration::get().quantization_bits);
        return quant * (quantized_score + 1);
    }
};

enum class PayloadType : bool { Float = false, Quantized = true };

template <PayloadType IndexPayloadType = PayloadType::Float>
class wand_data_compressed {
  public:
    class builder {
      public:
        builder(binary_freq_collection const& coll, global_parameters const& params)
            : total_elements(0),
              total_blocks(0),
              params(params),
              compressor_builder(coll.num_docs(), params)
        {
            spdlog::info("Storing max weight for each list and for each block...");
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

            float max_score = *(std::max_element(t.second.begin(), t.second.end()));
            max_term_weight.push_back(max_score);
            total_elements += seq.docs.size();
            total_blocks += t.first.size();

            block_max_documents.push_back(std::move(t.first));
            unquantized_block_max_scores.push_back(std::move(t.second));

            return max_term_weight.back();
        }

        void quantize_block_max_term_weights([[maybe_unused]] float index_max_term_weight) {}

        void build(wand_data_compressed& wdata)
        {
            auto index_max_term_weight =
                *(std::max_element(max_term_weight.begin(), max_term_weight.end()));
            for (auto&& [docs, scores]:
                 ranges::views::zip(block_max_documents, unquantized_block_max_scores)) {
                auto quantized_scores =
                    compressor_builder.compress_data(scores, index_max_term_weight);
                compressor_builder.add_posting_list(
                    quantized_scores.size(), docs.begin(), quantized_scores.begin());
            }
            wdata.m_num_docs = compressor_builder.num_docs();
            wdata.m_params = compressor_builder.params();
            compressor_builder.build(wdata.m_docs_sequences);
            spdlog::info(
                "number of elements / number of blocks: {}",
                (float)total_elements / (float)total_blocks);
        }

        uint64_t total_elements;
        uint64_t total_blocks;
        std::vector<std::vector<uint32_t>> block_max_documents;
        std::vector<std::vector<float>> unquantized_block_max_scores;
        std::vector<float> max_term_weight;
        global_parameters const& params;
        typename uniform_score_compressor::builder compressor_builder;
    };

    class enumerator {
        friend class wand_data_compressed;

      public:
        enumerator(compact_elias_fano::enumerator docs_enum, float max_term_weight)
            : m_docs_enum(docs_enum), m_max_term_weight(max_term_weight)
        {
            reset();
        }

        void reset()
        {
            uint64_t val = m_docs_enum.move(0).second;
            m_cur_docid = val >> score_bits_size;
            uint64_t mask = (1U << configuration::get().quantization_bits) - 1;
            m_cur_score_index = (val & mask);
        }

        void PISA_FLATTEN_FUNC next_geq(uint64_t lower_bound)
        {
            if (docid() != lower_bound) {
                lower_bound = lower_bound << score_bits_size;
                auto val = m_docs_enum.next_geq(lower_bound);
                m_cur_docid = val.second >> score_bits_size;
                uint64_t mask = (1U << configuration::get().quantization_bits) - 1;
                m_cur_score_index = (val.second & mask);
            }
        }

        float PISA_FLATTEN_FUNC score()
        {
            // NOLINTNEXTLINE(readability-braces-around-statements)
            if constexpr (IndexPayloadType == PayloadType::Quantized) {
                return m_cur_score_index;
            } else {
                return uniform_score_compressor::score(m_cur_score_index) * m_max_term_weight;
            }
        }

        uint64_t PISA_FLATTEN_FUNC docid() const { return m_cur_docid; }

      private:
        compact_elias_fano::enumerator m_docs_enum;
        float m_max_term_weight{0};
        uint64_t m_cur_docid{0};
        uint64_t m_cur_score_index{0};
    };

    uint64_t size() const { return m_docs_sequences.size(); }

    uint64_t num_docs() const { return m_num_docs; }

    enumerator get_enum(size_t i, float max_term_weight) const
    {
        assert(i < size());
        auto docs_it = m_docs_sequences.get(m_params, i);

        uint64_t n = read_gamma_nonzero(docs_it);
        typename compact_elias_fano::enumerator docs_enum(
            m_docs_sequences.bits(), docs_it.position(), num_docs(), n, m_params);

        return enumerator(docs_enum, max_term_weight);
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_params, "m_params")(m_num_docs, "m_num_docs")(m_docs_sequences, "m_docs_sequences");
    }

  private:
    global_parameters m_params;
    uint64_t m_num_docs{0};
    bitvector_collection m_docs_sequences;
};

}  // namespace pisa
