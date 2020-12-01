#pragma once

#include "bit_vector.hpp"
#include "mappable/mappable_vector.hpp"
#include "mappable/mapper.hpp"

#include "block_posting_list.hpp"
#include "codec/compact_elias_fano.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "temporary_directory.hpp"

namespace pisa {

struct BlockIndexTag;

template <typename BlockCodec, bool Profile = false>
class block_freq_index {
  public:
    using index_layout_tag = BlockIndexTag;
    block_freq_index() = default;
    explicit block_freq_index(MemorySource source) : m_source(std::move(source))
    {
        mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
    }

    class builder {
      public:
        builder(uint64_t num_docs, global_parameters const& params) : m_params(params)
        {
            m_num_docs = num_docs;
            m_endpoints.push_back(0);
        }

        template <typename DocsIterator, typename FreqsIterator>
        void add_posting_list(
            uint64_t n,
            DocsIterator docs_begin,
            FreqsIterator freqs_begin,
            uint64_t /* occurrences */)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            block_posting_list<BlockCodec, Profile>::write(m_lists, n, docs_begin, freqs_begin);
            m_endpoints.push_back(m_lists.size());
        }

        template <typename BlockDataRange>
        void add_posting_list(uint64_t n, BlockDataRange const& blocks)
        {
            if (!n) {
                throw std::invalid_argument("List must be nonempty");
            }
            block_posting_list<BlockCodec>::write_blocks(m_lists, n, blocks);
            m_endpoints.push_back(m_lists.size());
        }

        template <typename BytesRange>
        void add_posting_list(BytesRange const& data)
        {
            m_lists.insert(m_lists.end(), std::begin(data), std::end(data));
            m_endpoints.push_back(m_lists.size());
        }

        void build(block_freq_index& sq)
        {
            sq.m_params = m_params;
            sq.m_size = m_endpoints.size() - 1;
            sq.m_num_docs = m_num_docs;
            sq.m_lists.steal(m_lists);

            bit_vector_builder bvb;
            compact_elias_fano::write(
                bvb,
                m_endpoints.begin(),
                sq.m_lists.size(),
                sq.m_size,
                m_params);  // XXX
            bit_vector(&bvb).swap(sq.m_endpoints);
        }

      private:
        global_parameters m_params;
        size_t m_num_docs;
        std::vector<uint64_t> m_endpoints;
        std::vector<uint8_t> m_lists;
    };

    class stream_builder {
      public:
        stream_builder(uint64_t num_docs, global_parameters const& params)
            : m_params(params), m_postings_output((tmp.path() / "buffer").c_str())
        {
            m_num_docs = num_docs;
            m_endpoints.push_back(0);
        }

        template <typename DocsIterator, typename FreqsIterator>
        void add_posting_list(
            uint64_t n,
            DocsIterator docs_begin,
            FreqsIterator freqs_begin,
            uint64_t /* occurrences */)
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

        template <typename BlockDataRange>
        void add_posting_list(uint64_t n, BlockDataRange const& blocks)
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

        template <typename BytesRange>
        void add_posting_list(BytesRange const& data)
        {
            m_postings_bytes_written += data.size();
            m_postings_output.write(reinterpret_cast<char const*>(data.data()), data.size());
            m_endpoints.push_back(m_postings_bytes_written);
        }

        void build(std::string const& index_path)
        {
            std::ofstream os(index_path.c_str());
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

            std::ifstream buf((tmp.path() / "buffer").c_str());
            m_postings_output.close();
            os.write(
                reinterpret_cast<char const*>(&m_postings_bytes_written),
                sizeof(m_postings_bytes_written));
            os << buf.rdbuf();
        }

      private:
        global_parameters m_params{};
        size_t m_num_docs = 0;
        size_t m_size = 0;
        std::vector<uint64_t> m_endpoints{};
        Temporary_Directory tmp{};
        std::ofstream m_postings_output;
        std::size_t m_postings_bytes_written{0};
    };

    size_t size() const { return m_size; }

    uint64_t num_docs() const { return m_num_docs; }

    using document_enumerator = typename block_posting_list<BlockCodec, Profile>::document_enumerator;

    document_enumerator operator[](size_t i) const
    {
        assert(i < size());
        compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);

        auto endpoint = endpoints.move(i).second;
        return document_enumerator(m_lists.data() + endpoint, num_docs(), i);
    }

    void warmup(size_t i) const
    {
        assert(i < size());
        compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);

        auto begin = endpoints.move(i).second;
        auto end = m_lists.size();
        if (i + 1 != size()) {
            end = endpoints.move(i + 1).second;
        }

        volatile uint32_t tmp;
        for (size_t i = begin; i != end; ++i) {
            tmp = m_lists[i];
        }
        (void)tmp;
    }

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
    size_t m_size{0};
    size_t m_num_docs{0};
    bit_vector m_endpoints;
    mapper::mappable_vector<uint8_t> m_lists;
    MemorySource m_source;
};
}  // namespace pisa
