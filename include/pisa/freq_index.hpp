#pragma once

#include <fmt/format.h>
#include <tbb/parallel_invoke.h>

#include "bitvector_collection.hpp"
#include "codec/compact_elias_fano.hpp"
#include "codec/integer_codes.hpp"
#include "global_parameters.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"

namespace pisa {

/**
 * Used as a tag for index layout.
 */
struct BitVectorIndexTag;

/**
 * Bit-vector based frequency inverted index.
 *
 * Each posting list is part of two bit vectors: one for document IDs,
 * and the other for frequencies (or scores).
 *
 * \tparam DocsSequence   Type of sequence used to encode documents in a bit vector.
 * \tparam FreqsSequence  Type of sequence used to encode frequencies in a bit vector.
 */
template <typename DocsSequence, typename FreqsSequence>
class freq_index {
  public:
    using index_layout_tag = BitVectorIndexTag;

    freq_index() = default;

    /**
     * Constructs a view over the index encoded in the given memory source.
     *
     * \param source  Holds the bytes encoding the index. The source must be valid
     *                throughout the life of the index. Once the source gets deallocated,
     *                any index operations may result in undefined behavior.
     */
    explicit freq_index(MemorySource source) : m_source(std::move(source))
    {
        mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
    }

    /**
     * In-memory index builder.
     *
     * Builds the index in memory, which is eventually flushed to disk at the very end.
     */
    class builder {
      public:
        /**
         * Constructs a builder for an index containing the given number of documents.
         */
        builder(uint64_t num_docs, global_parameters const& params)
            : m_params(params),
              m_num_docs(num_docs),
              m_docs_sequences(params),
              m_freqs_sequences(params)
        {}

        /**
         * Records a new posting list.
         *
         * Postings are written to an in-memory buffer.
         *
         * \param n            Posting list length.
         * \param docs_begin   Iterator that points at the first document ID.
         * \param freqs_begin  Iterator that points at the first frequency.
         * \param occurrences  Not used in this builder.
         *
         * \throws std::invalid_argument   Thrown if `n == 0`.
         */
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

        /**
         * Builds an index.
         *
         * \param sq  Inverted index object that will take ownership of the data.
         */
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

    /**
     * \returns  The size of the index, i.e., the number of terms (posting lists).
     */
    [[nodiscard]] std::size_t size() const noexcept { return m_docs_sequences.size(); }

    /**
     * \returns  The number of distinct documents in the index.
     */
    [[nodiscard]] std::uint64_t num_docs() const noexcept { return m_num_docs; }

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

    /**
     * Accesses the given posting list.
     *
     * \params term_id  The term ID, must be a positive integer lower than the index size.
     *
     * \throws std::out_of_range  Thrown if term ID is greater than or equal to
     *                            number of terms in the index.
     *
     * \returns  The cursor over the posting list.
     */
    [[nodiscard]] document_enumerator operator[](size_t term_id) const
    {
        if (term_id >= size()) {
            throw std::out_of_range(
                fmt::format("given term ID ({}) is out of range, must be < {}", term_id, size()));
        }
        auto docs_it = m_docs_sequences.get(m_params, term_id);
        uint64_t occurrences = read_gamma_nonzero(docs_it);
        uint64_t n = 1;
        if (occurrences > 1) {
            n = docs_it.take(ceil_log2(occurrences + 1));
        }

        typename DocsSequence::enumerator docs_enum(
            m_docs_sequences.bits(), docs_it.position(), num_docs(), n, m_params);

        auto freqs_it = m_freqs_sequences.get(m_params, term_id);
        typename FreqsSequence::enumerator freqs_enum(
            m_freqs_sequences.bits(), freqs_it.position(), occurrences + 1, n, m_params);

        return document_enumerator(docs_enum, freqs_enum);
    }

    /**
     * No-op.
     */
    void warmup(size_t /* i */) const
    {
        // XXX implement this
    }

    global_parameters const& params() const { return m_params; }

    /**
     * Swaps all data with another index.
     */
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
        visit(m_params, "m_params")  //
            (m_num_docs, "m_num_docs")  //
            (m_docs_sequences, "m_docs_sequences")  //
            (m_freqs_sequences, "m_freqs_sequences");
    }

  private:
    global_parameters m_params;
    uint64_t m_num_docs = 0;
    bitvector_collection m_docs_sequences;
    bitvector_collection m_freqs_sequences;
    MemorySource m_source;
};

}  // namespace pisa
