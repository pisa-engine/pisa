#include "block_inverted_index.hpp"
#include "codec/compact_elias_fano.hpp"
#include "mappable/mapper.hpp"

namespace pisa {

BlockInvertedIndex::BlockInvertedIndex(MemorySource source, std::unique_ptr<BlockCodec> block_codec)
    : m_source(std::move(source)), m_block_codec(std::move(block_codec)) {
    PISA_ASSERT_CONCEPT((concepts::SortedInvertedIndex<BlockInvertedIndex, BlockInvertedIndexCursor>));
    mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
}

auto BlockInvertedIndex::operator[](std::size_t term_id) const -> BlockInvertedIndexCursor {
    check_term_range(term_id);
    compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);
    auto endpoint = endpoints.move(term_id).second;
    return BlockInvertedIndexCursor(m_block_codec.get(), m_lists.data() + endpoint, num_docs());
}

void BlockInvertedIndex::check_term_range(std::size_t term_id) const {
    if (term_id >= size()) {
        throw std::out_of_range(
            fmt::format("given term ID ({}) is out of range, must be < {}", term_id, size())
        );
    }
}

void BlockInvertedIndex::warmup(std::size_t term_id) const {
    check_term_range(term_id);
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

index::block::InMemoryBuilder::InMemoryBuilder(
    std::uint64_t num_docs, global_parameters const& params, BlockPostingWriter posting_writer
)
    : m_params(params), m_num_docs(num_docs), m_posting_writer(posting_writer) {
    m_endpoints.push_back(0);
}

void index::block::InMemoryBuilder::build(BlockInvertedIndex& sq) {
    sq.m_params = m_params;
    sq.m_size = m_endpoints.size() - 1;
    sq.m_num_docs = m_num_docs;

    // This is a workaround to QMX codex having to sometimes look beyond the buffer
    // due to some SIMD loads.
    std::array<char, 15> padding{};
    m_lists.insert(m_lists.end(), padding.begin(), padding.end());
    sq.m_lists.steal(m_lists);

    bit_vector_builder bvb;
    compact_elias_fano::write(bvb, m_endpoints.begin(), sq.m_lists.size(), sq.m_size, m_params);
    bit_vector(&bvb).swap(sq.m_endpoints);
}

index::block::StreamBuilder::StreamBuilder(
    std::uint64_t num_docs, global_parameters const& params, BlockPostingWriter posting_writer
)
    : m_params(params),
      m_postings_output((tmp.path() / "buffer").c_str()),
      m_posting_writer(posting_writer) {
    m_postings_output.exceptions(std::ios::badbit | std::ios::failbit);
    m_num_docs = num_docs;
    m_endpoints.push_back(0);
}

void index::block::StreamBuilder::build(std::string const& index_path) {
    // This is a workaround to QMX codex having to sometimes look beyond the buffer
    // due to some SIMD loads.
    std::array<char, 15> padding{};
    m_postings_output.write(padding.data(), padding.size());
    m_postings_bytes_written += padding.size();

    std::ofstream os(index_path.c_str());
    std::cout << index_path.c_str() << "\n";
    os.exceptions(std::ios::badbit | std::ios::failbit);
    mapper::detail::freeze_visitor freezer(os, 0);
    freezer(m_params, "m_params");
    std::size_t size = m_endpoints.size() - 1;
    freezer(size, "size");
    freezer(m_num_docs, "m_num_docs");

    bit_vector_builder bvb;
    compact_elias_fano::write(bvb, m_endpoints.begin(), m_postings_bytes_written, size, m_params);
    bit_vector endpoints(&bvb);
    freezer(endpoints, "endpoints");

    m_postings_output.close();
    std::ifstream buf((tmp.path() / "buffer").c_str());
    buf.exceptions(std::ios::badbit);
    os.write(
        reinterpret_cast<char const*>(&m_postings_bytes_written), sizeof(m_postings_bytes_written)
    );
    os << buf.rdbuf();
}

}  // namespace pisa
