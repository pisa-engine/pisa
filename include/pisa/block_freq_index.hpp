#pragma once

#include "bit_vector.hpp"
#include "block_posting_list.hpp"
#include "codec/compact_elias_fano.hpp"
#include "mappable/mappable_vector.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "temporary_directory.hpp"

namespace pisa {

/**
 * Used as a tag for index layout.
 */
struct BlockIndexTag;

/**
 * Blocked frequency inverted index.
 *
 * Each posting list is divided into blocks, and each block is encoded separately.
 * One block contains both document IDs and frequencies (or scores).
 *
 * \tparam BlockCodec  Block-wise codec type.
 * \tparam Profile     If true, enables performance profiling.
 */
template <typename BlockCodec, bool Profile = false>
class block_freq_index {
  public:
    using index_layout_tag = BlockIndexTag;

    block_freq_index() = default;

    /**
     * Constructs a view over the index encoded in the given memory source.
     *
     * \param source  Holds the bytes encoding the index. The source must be valid
     *                throughout the life of the index. Once the source gets deallocated,
     *                any index operations may result in undefined behavior.
     */
    explicit block_freq_index(MemorySource source) : m_source(std::move(source))
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
        builder(std::uint64_t num_docs, global_parameters const& params) : m_params(params)
        {
            m_num_docs = num_docs;
            m_endpoints.push_back(0);
        }

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
            std::uint64_t n,
            DocsIterator docs_begin,
            FreqsIterator freqs_begin,
            std::uint64_t /* occurrences */)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            block_posting_list<BlockCodec, Profile>::write(m_lists, n, docs_begin, freqs_begin);
            m_endpoints.push_back(m_lists.size());
        }

        /**
         * Adds multiple posting list blocks.
         *
         * \tparam BlockDataRange  This intended to be a vector of structs of the type
         *      block_posting_list<BlockCodec, Profile>::document_enumerator::block_data
         *
         * \param n       Posting list length.
         * \param blocks  Encoded blocks.
         *
         * \throws std::invalid_argument   Thrown if `n == 0`.
         */
        template <typename BlockDataRange>
        void add_posting_list(std::uint64_t n, BlockDataRange const& blocks)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            block_posting_list<BlockCodec>::write_blocks(m_lists, n, blocks);
            m_endpoints.push_back(m_lists.size());
        }

        /**
         * Adds a posting list that is already fully encoded.
         *
         * \tparam BytesRange  A collection of bytes, e.g., std::vector<std::uint8_t>
         *
         * \param data  Encoded data.
         */
        template <typename BytesRange>
        void add_posting_list(BytesRange const& data)
        {
            m_lists.insert(m_lists.end(), std::begin(data), std::end(data));
            m_endpoints.push_back(m_lists.size());
        }

        /**
         * Builds an index.
         *
         * \param sq  Inverted index object that will take ownership of the data.
         */
        void build(block_freq_index& sq)
        {
            sq.m_params = m_params;
            sq.m_size = m_endpoints.size() - 1;
            sq.m_num_docs = m_num_docs;
            sq.m_lists.steal(m_lists);

            bit_vector_builder bvb;
            compact_elias_fano::write(
                bvb, m_endpoints.begin(), sq.m_lists.size(), sq.m_size, m_params);
            bit_vector(&bvb).swap(sq.m_endpoints);
        }

      private:
        global_parameters m_params;
        std::size_t m_num_docs;
        std::vector<std::uint64_t> m_endpoints;
        std::vector<std::uint8_t> m_lists;
    };

    /**
     * Stream index builder.
     *
     * Buffers postings on disk in order to support building indexes larger than memory.
     */
    class stream_builder {
      public:
        /**
         * Constructs a builder for an index containing the given number of documents.
         *
         * This constructor opens a temporary file to write. This file is used for
         * buffering postings. This buffer is flushed to the right destination when the
         * `build` member function is called.
         *
         * \throws std::ios_base::failure Thrown if the the temporary buffer file cannot be opened.
         */
        stream_builder(std::uint64_t num_docs, global_parameters const& params)
            : m_params(params), m_postings_output((tmp.path() / "buffer").c_str())
        {
            m_postings_output.exceptions(std::ios::badbit | std::ios::failbit);
            m_num_docs = num_docs;
            m_endpoints.push_back(0);
        }

        /**
         * Records a new posting list.
         *
         * Postings are written to the temporary file, while some other data is accumulated
         * within the builder struct.
         *
         * \param n            Posting list length.
         * \param docs_begin   Iterator that points at the first document ID.
         * \param freqs_begin  Iterator that points at the first frequency.
         * \param occurrences  Not used in this builder.
         *
         * \throws std::invalid_argument   Thrown if `n == 0`.
         * \throws std::ios_base::failure  Thrown if failed to write to the temporary file buffer.
         */
        template <typename DocsIterator, typename FreqsIterator>
        void add_posting_list(
            std::uint64_t n,
            DocsIterator docs_begin,
            FreqsIterator freqs_begin,
            std::uint64_t /* occurrences */)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            std::vector<std::uint8_t> buf;
            block_posting_list<BlockCodec, Profile>::write(buf, n, docs_begin, freqs_begin);
            m_postings_bytes_written += buf.size();
            m_postings_output.write(reinterpret_cast<char const*>(buf.data()), buf.size());
            m_endpoints.push_back(m_postings_bytes_written);
        }

        /**
         * Adds multiple posting list blocks.
         *
         * \tparam BlockDataRange  This intended to be a vector of structs of the type
         *      block_posting_list<BlockCodec, Profile>::document_enumerator::block_data
         *
         * \param n       Posting list length.
         * \param blocks  Encoded blocks.
         *
         * \throws std::invalid_argument   Thrown if `n == 0`.
         * \throws std::ios_base::failure  Thrown if failed to write to the temporary file buffer.
         */
        template <typename BlockDataRange>
        void add_posting_list(std::uint64_t n, BlockDataRange const& blocks)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            std::vector<std::uint8_t> buf;
            block_posting_list<BlockCodec>::write_blocks(buf, n, blocks);
            m_postings_bytes_written += buf.size();
            m_postings_output.write(reinterpret_cast<char const*>(buf.data()), buf.size());
            m_endpoints.push_back(m_postings_bytes_written);
        }

        /**
         * Adds a posting list that is already fully encoded.
         *
         * \tparam BytesRange  A collection of bytes, e.g., std::vector<std::uint8_t>
         *
         * \param data  Encoded data.
         *
         * \throws std::ios_base::failure  Thrown if failed to write to the temporary file buffer.
         */
        template <typename BytesRange>
        void add_posting_list(BytesRange const& data)
        {
            m_postings_bytes_written += data.size();
            m_postings_output.write(reinterpret_cast<char const*>(data.data()), data.size());
            m_endpoints.push_back(m_postings_bytes_written);
        }

        /**
         * Flushes index data to disk.
         *
         * \param index_path  Output index path.
         *
         * \throws std::ios_base::failure  Thrown if failed to write to any file
         *                                 or failed to read from the temporary buffer.
         */
        void build(std::string const& index_path)
        {
            std::ofstream os(index_path.c_str());
            os.exceptions(std::ios::badbit | std::ios::failbit);
            mapper::detail::freeze_visitor freezer(os, 0);
            freezer(m_params, "m_params");
            std::size_t size = m_endpoints.size() - 1;
            freezer(size, "size");
            freezer(m_num_docs, "m_num_docs");

            bit_vector_builder bvb;
            compact_elias_fano::write(
                bvb, m_endpoints.begin(), m_postings_bytes_written, size, m_params);
            bit_vector endpoints(&bvb);
            freezer(endpoints, "endpoints");

            m_postings_output.close();
            std::ifstream buf((tmp.path() / "buffer").c_str());
            buf.exceptions(std::ios::badbit);
            os.write(
                reinterpret_cast<char const*>(&m_postings_bytes_written),
                sizeof(m_postings_bytes_written));
            os << buf.rdbuf();
        }

      private:
        global_parameters m_params{};
        std::size_t m_num_docs = 0;
        std::size_t m_size = 0;
        std::vector<std::uint64_t> m_endpoints{};
        Temporary_Directory tmp{};
        std::ofstream m_postings_output;
        std::size_t m_postings_bytes_written{0};
    };

    /**
     * \returns  The size of the index, i.e., the number of terms (posting lists).
     */
    [[nodiscard]] std::size_t size() const noexcept { return m_size; }

    /**
     * \returns  The number of distinct documents in the index.
     */
    [[nodiscard]] std::uint64_t num_docs() const noexcept { return m_num_docs; }

    using document_enumerator = typename block_posting_list<BlockCodec, Profile>::document_enumerator;

    /**
     * Accesses the given posting list.
     *
     * \params term_id  The term ID, must be a positive integer lower than the index size.
     *                  Providing invlid value results in an undefined behavior.
     *
     * \returns  The cursor over the posting list.
     */
    [[nodiscard]] document_enumerator operator[](std::size_t term_id) const
    {
        assert(term_id < size());
        compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);
        auto endpoint = endpoints.move(term_id).second;
        return document_enumerator(m_lists.data() + endpoint, num_docs(), term_id);
    }

    /**
     * Iterates over the postings in the given list in order to bring the data to memory cache.
     *
     * \params term_id  The term ID, must be a positive integer lower than the index size.
     *                  Providing invlid value results in an undefined behavior.
     */
    void warmup(std::size_t term_id) const
    {
        assert(term_id < size());
        compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);

        auto begin = endpoints.move(term_id).second;
        auto end = m_lists.size();
        if (term_id + 1 != size()) {
            end = endpoints.move(term_id + 1).second;
        }

        volatile std::uint32_t tmp;
        for (std::size_t i = begin; i != end; ++i) {
            tmp = m_lists[i];
        }
        (void)tmp;
    }

    /**
     * Swaps all data with another index.
     */
    void swap(block_freq_index& other)
    {
        std::swap(m_params, other.m_params);
        std::swap(m_size, other.m_size);
        m_endpoints.swap(other.m_endpoints);
        m_lists.swap(other.m_lists);
    }

    template <typename Visitor>
    void map(Visitor& visit)
    {
        visit(m_params, "m_params")(m_size, "m_size")(m_num_docs, "m_num_docs")(
            m_endpoints, "m_endpoints")(m_lists, "m_lists");
    }

  private:
    global_parameters m_params;
    std::size_t m_size{0};
    std::size_t m_num_docs{0};
    bit_vector m_endpoints;
    mapper::mappable_vector<std::uint8_t> m_lists;
    MemorySource m_source;
};

}  // namespace pisa
