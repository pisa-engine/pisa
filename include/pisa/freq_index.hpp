#pragma once

#include "tbb/parallel_invoke.h"

#include "bitvector_collection.hpp"
#include "codec/compact_elias_fano.hpp"
#include "codec/integer_codes.hpp"
#include "global_parameters.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"

namespace pisa {

struct BitVectorIndexTag;

template <typename DocsSequence, typename FreqsSequence>
class freq_index {
  public:
    using index_layout_tag = BitVectorIndexTag;

    freq_index() = default;

    explicit freq_index(MemorySource source) : m_source(std::move(source))
    {
        mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
    }

    class builder {
      public:
        builder(uint64_t num_docs, global_parameters const& params)
            : m_params(params),
              m_num_docs(num_docs),
              m_docs_sequences(params),
              m_freqs_sequences(params)
        {}

        template <typename DocsIterator, typename FreqsIterator>
        void add_posting_list(
            uint64_t n, DocsIterator docs_begin, FreqsIterator freqs_begin, uint64_t occurrences)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }

            tbb::parallel_invoke(
                [&] {
                    bit_vector_builder docs_bits;
                    write_gamma_nonzero(docs_bits, occurrences);
                    if (occurrences > 1) {
                        docs_bits.append_bits(n, ceil_log2(occurrences + 1));
                    }
                    DocsSequence::write(docs_bits, docs_begin, m_num_docs, n, m_params);
                    m_docs_sequences.append(docs_bits);
                },
                [&] {
                    bit_vector_builder freqs_bits;
                    FreqsSequence::write(freqs_bits, freqs_begin, occurrences + 1, n, m_params);
                    m_freqs_sequences.append(freqs_bits);
                });
        }

        void build(freq_index& sq)
        {
            sq.m_num_docs = m_num_docs;
            sq.m_params = m_params;

            m_docs_sequences.build(sq.m_docs_sequences);
            m_freqs_sequences.build(sq.m_freqs_sequences);
        }

      private:
        global_parameters m_params;
        uint64_t m_num_docs = 0;
        bitvector_collection::builder m_docs_sequences;
        bitvector_collection::builder m_freqs_sequences;
    };

    uint64_t size() const { return m_docs_sequences.size(); }

    uint64_t num_docs() const { return m_num_docs; }

    class document_enumerator {
      public:
        void reset()
        {
            m_cur_pos = 0;
            m_cur_docid = m_docs_enum.move(0).second;
        }

        void PISA_FLATTEN_FUNC next()
        {
            auto val = m_docs_enum.next();
            m_cur_pos = val.first;
            m_cur_docid = val.second;
        }

        void PISA_FLATTEN_FUNC next_geq(uint64_t lower_bound)
        {
            auto val = m_docs_enum.next_geq(lower_bound);
            m_cur_pos = val.first;
            m_cur_docid = val.second;
        }

        void PISA_FLATTEN_FUNC move(uint64_t position)
        {
            auto val = m_docs_enum.move(position);
            m_cur_pos = val.first;
            m_cur_docid = val.second;
        }

        uint64_t docid() const { return m_cur_docid; }

        uint64_t PISA_FLATTEN_FUNC freq() { return m_freqs_enum.move(m_cur_pos).second; }

        uint64_t position() const { return m_cur_pos; }

        uint64_t size() const { return m_docs_enum.size(); }

        typename DocsSequence::enumerator const& docs_enum() const { return m_docs_enum; }

        typename FreqsSequence::enumerator const& freqs_enum() const { return m_freqs_enum; }

      private:
        friend class freq_index;

        document_enumerator(
            typename DocsSequence::enumerator docs_enum, typename FreqsSequence::enumerator freqs_enum)
            : m_docs_enum(docs_enum), m_freqs_enum(freqs_enum)
        {
            reset();
        }

        uint64_t m_cur_pos{0};
        uint64_t m_cur_docid{0};
        typename DocsSequence::enumerator m_docs_enum;
        typename FreqsSequence::enumerator m_freqs_enum;
    };

    document_enumerator operator[](size_t i) const
    {
        assert(i < size());
        auto docs_it = m_docs_sequences.get(m_params, i);
        uint64_t occurrences = read_gamma_nonzero(docs_it);
        uint64_t n = 1;
        if (occurrences > 1) {
            n = docs_it.take(ceil_log2(occurrences + 1));
        }

        typename DocsSequence::enumerator docs_enum(
            m_docs_sequences.bits(), docs_it.position(), num_docs(), n, m_params);

        auto freqs_it = m_freqs_sequences.get(m_params, i);
        typename FreqsSequence::enumerator freqs_enum(
            m_freqs_sequences.bits(), freqs_it.position(), occurrences + 1, n, m_params);

        return document_enumerator(docs_enum, freqs_enum);
    }

    void warmup(size_t /* i */) const
    {
        // XXX implement this
    }

    global_parameters const& params() const { return m_params; }

    void swap(freq_index& other)
    {
        std::swap(m_params, other.m_params);
        std::swap(m_num_docs, other.m_num_docs);
        m_docs_sequences.swap(other.m_docs_sequences);
        m_freqs_sequences.swap(other.m_freqs_sequences);
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_params, "m_params")(m_num_docs, "m_num_docs")(m_docs_sequences, "m_docs_sequences")(
            m_freqs_sequences, "m_freqs_sequences");
    }

  private:
    global_parameters m_params;
    uint64_t m_num_docs = 0;
    bitvector_collection m_docs_sequences;
    bitvector_collection m_freqs_sequences;
    MemorySource m_source;
};
}  // namespace pisa
