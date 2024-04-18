#include "block_inverted_index.hpp"
#include "bit_vector_builder.hpp"
#include "codec/compact_elias_fano.hpp"
#include "mappable/mapper.hpp"
#include "util/progress.hpp"
#include "util/verify_collection.hpp"

namespace pisa {

BlockInvertedIndex::BlockInvertedIndex(MemorySource source, BlockCodecPtr block_codec)
    : m_source(std::move(source)), m_block_codec(std::move(block_codec)) {
    PISA_ASSERT_CONCEPT((concepts::SortedInvertedIndex<BlockInvertedIndex, BlockInvertedIndexCursor<>>));
    mapper::map(*this, m_source.data(), mapper::map_flags::warmup);
}

BlockInvertedIndex::BlockInvertedIndex(BlockCodecPtr block_codec)
    : m_block_codec(std::move(block_codec)) {
    PISA_ASSERT_CONCEPT((concepts::SortedInvertedIndex<BlockInvertedIndex, BlockInvertedIndexCursor<>>));
}

auto BlockInvertedIndex::operator[](std::size_t term_id) const -> BlockInvertedIndexCursor<> {
    check_term_range(term_id);
    compact_elias_fano::enumerator endpoints(m_endpoints, 0, m_lists.size(), m_size, m_params);
    auto endpoint = endpoints.move(term_id).second;
    return BlockInvertedIndexCursor(
        m_block_codec.get(), m_lists.data() + endpoint, num_docs(), term_id
    );
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

ProfilingBlockInvertedIndex::ProfilingBlockInvertedIndex(MemorySource source, BlockCodecPtr block_codec)
    : BlockInvertedIndex(std::move(source), std::move(block_codec)) {
    PISA_ASSERT_CONCEPT(
        (concepts::SortedInvertedIndex<ProfilingBlockInvertedIndex, BlockInvertedIndexCursor<Profiling::On>>)
    );
}

index::block::PostingAccumulator::PostingAccumulator(
    BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
)
    : m_block_codec(std::move(block_codec)),
      m_num_docs(num_docs),
      m_output_filename(std::move(output_filename)) {}

void index::block::PostingAccumulator::write(
    std::vector<uint8_t>& out, std::uint32_t n, std::uint32_t const* docs, std::uint32_t const* freqs
) {
    TightVariableByte::encode_single(n, out);

    uint64_t block_size = m_block_codec->block_size();
    uint64_t blocks = ceil_div(n, block_size);
    size_t begin_block_maxs = out.size();
    size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
    size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
    out.resize(begin_blocks);

    std::vector<uint32_t> docs_buf(block_size);
    std::vector<uint32_t> freqs_buf(block_size);
    int32_t last_doc(-1);
    uint32_t block_base = 0;
    for (size_t b = 0; b < blocks; ++b) {
        uint32_t cur_block_size = ((b + 1) * block_size <= n) ? block_size : (n % block_size);

        for (size_t i = 0; i < cur_block_size; ++i) {
            uint32_t doc(*docs++);
            docs_buf[i] = doc - last_doc - 1;
            last_doc = doc;

            freqs_buf[i] = *freqs++ - 1;
        }
        *((uint32_t*)&out[begin_block_maxs + 4 * b]) = last_doc;

        m_block_codec->encode(
            docs_buf.data(), last_doc - block_base - (cur_block_size - 1), cur_block_size, out
        );
        m_block_codec->encode(freqs_buf.data(), uint32_t(-1), cur_block_size, out);
        if (b != blocks - 1) {
            *((uint32_t*)&out[begin_block_endpoints + 4 * b]) = out.size() - begin_blocks;
        }
        block_base = last_doc + 1;
    }
}

BlockIndexBuilder::BlockIndexBuilder(
    binary_freq_collection input, BlockCodecPtr block_codec, ScorerParams scorer_params
)
    : m_input(std::move(input)),
      m_block_codec(std::move(block_codec)),
      m_scorer_params(scorer_params) {}

auto BlockIndexBuilder::check(bool check) -> BlockIndexBuilder& {
    m_check = check;
    return *this;
}

auto BlockIndexBuilder::in_memory(bool in_mem) -> BlockIndexBuilder& {
    m_in_memory = in_mem;
    return *this;
}

void BlockIndexBuilder::build(std::string const& index_path) {
    spdlog::info("Processing {} documents", m_input.num_docs());
    double tick = get_time_usecs();

    size_t postings = 0;
    {
        auto accumulator = [&]() -> std::unique_ptr<index::block::PostingAccumulator> {
            if (m_in_memory) {
                return std::make_unique<index::block::InMemoryPostingAccumulator>(
                    m_block_codec, m_input.num_docs(), index_path
                );
            }
            return std::make_unique<index::block::StreamPostingAccumulator>(
                m_block_codec, m_input.num_docs(), index_path
            );
        }();

        pisa::progress progress("Create index", m_input.size());

        size_t term_id = 0;
        for (auto const& plist: m_input) {
            size_t size = plist.docs.size();
            if (m_quantizing_scorer.has_value()) {
                auto term_scorer = m_quantizing_scorer->term_scorer(term_id);
                std::vector<std::uint32_t> quants;
                for (size_t pos = 0; pos < size; ++pos) {
                    std::uint32_t doc = *(plist.docs.begin() + pos);
                    std::uint32_t freq = *(plist.freqs.begin() + pos);
                    std::uint32_t quant_score = term_scorer(doc, freq);
                    quants.push_back(quant_score);
                }
                assert(quants.size() == size);
                accumulator->accumulate_posting_list(size, plist.docs.begin(), &quants[0]);
            } else {
                accumulator->accumulate_posting_list(size, plist.docs.begin(), plist.freqs.begin());
            }

            progress.update(1);
            postings += size;
            term_id += 1;
        }
        accumulator->finish();
    }

    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("Index compressed in {} seconds", elapsed_secs);

    stats_line()("worker_threads", std::thread::hardware_concurrency())(
        "construction_time", elapsed_secs
    );
    (void)postings;
    // TODO
    // dump_stats(coll, seq_type, postings);
    // dump_index_specific_stats(coll, seq_type);

    if (m_check) {
        BlockInvertedIndex index(
            MemorySource::mapped_file(std::filesystem::path(index_path)), m_block_codec
        );
        verify_collection<binary_freq_collection, BlockInvertedIndex>(
            m_input, index, std::move(m_quantizing_scorer)
        );
    }
}

index::block::InMemoryPostingAccumulator::InMemoryPostingAccumulator(
    BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
)
    : index::block::PostingAccumulator(block_codec, num_docs, std::move(output_filename)) {
    m_endpoints.push_back(0);
}

void index::block::InMemoryPostingAccumulator::accumulate_posting_list(
    std::uint64_t n, std::uint32_t const* docs, std::uint32_t const* freqs
) {
    if (n == 0U) {
        throw std::invalid_argument("List must be nonempty");
    }
    write(m_lists, n, docs, freqs);
    m_endpoints.push_back(m_lists.size());
}

void index::block::InMemoryPostingAccumulator::finish() {
    m_finished = true;

    BlockInvertedIndex coll(m_block_codec);
    coll.m_params = m_params;
    coll.m_size = m_endpoints.size() - 1;
    coll.m_num_docs = m_num_docs;

    // This is a workaround to QMX codex having to sometimes look beyond the buffer
    // due to some SIMD loads.
    std::array<char, 15> padding{};
    m_lists.insert(m_lists.end(), padding.begin(), padding.end());
    coll.m_lists.steal(m_lists);

    bit_vector_builder bvb;
    compact_elias_fano::write(bvb, m_endpoints.begin(), coll.m_lists.size(), coll.m_size, m_params);
    bit_vector(&bvb).swap(coll.m_endpoints);
    mapper::freeze(coll, m_output_filename.c_str());
}

index::block::StreamPostingAccumulator::StreamPostingAccumulator(
    BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
)
    : index::block::PostingAccumulator(block_codec, num_docs, std::move(output_filename)),
      m_tmp_file(m_tmp.path() / "buffer"),
      m_postings_output(m_tmp_file) {}

void index::block::StreamPostingAccumulator::accumulate_posting_list(
    std::uint64_t n, std::uint32_t const* docs, std::uint32_t const* freqs
) {
    if (n == 0) {
        throw std::invalid_argument("List must be nonempty");
    }
    std::vector<std::uint8_t> buf;
    write(buf, n, docs, freqs);
    m_postings_bytes_written += buf.size();
    m_postings_output.write(reinterpret_cast<char const*>(buf.data()), buf.size());
    m_endpoints.push_back(m_postings_bytes_written);
}

void index::block::StreamPostingAccumulator::finish() {
    m_finished = true;

    // This is a workaround to QMX codex having to sometimes look beyond the buffer
    // due to some SIMD loads.
    std::array<char, 15> padding{};
    m_postings_output.write(padding.data(), padding.size());
    m_postings_bytes_written += padding.size();

    std::ofstream os(m_output_filename.c_str());
    std::cout << m_output_filename.c_str() << "\n";
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
    std::ifstream buf(m_tmp_file);
    buf.exceptions(std::ios::badbit);
    os.write(
        reinterpret_cast<char const*>(&m_postings_bytes_written), sizeof(m_postings_bytes_written)
    );
    os << buf.rdbuf();
    os.flush();
}

}  // namespace pisa
