#pragma once

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "binary_freq_collection.hpp"
#include "bit_vector.hpp"
#include "codec/block_codec.hpp"
#include "codec/block_codecs.hpp"
#include "concepts.hpp"
#include "concepts/inverted_index.hpp"
#include "global_parameters.hpp"
#include "mappable/mappable_vector.hpp"
#include "mappable/mapper.hpp"
#include "memory_source.hpp"
#include "scorer/quantized.hpp"
#include "scorer/scorer.hpp"
#include "temporary_directory.hpp"
#include "type_safe.hpp"
#include "util/block_profiler.hpp"

namespace pisa {

namespace index::block {
    // class InMemoryBuilder;
    class InMemoryPostingAccumulator;
    // class StreamBuilder;
    class StreamPostingAccumulator;
}  // namespace index::block

enum Profiling : bool { On, Off };

/**
 * Cursor for a block-encoded posting list.
 */
template <Profiling profiling = Profiling::Off>
class BlockInvertedIndexCursor {
  public:
    BlockInvertedIndexCursor(
        BlockCodec const* block_codec,
        std::uint8_t const* data,
        std::uint64_t universe,
        [[maybe_unused]] std::uint32_t term_id
    )
        : m_base(TightVariableByte::decode(data, &m_n, 1)),
          m_blocks(ceil_div(m_n, block_codec->block_size())),
          m_block_maxs(m_base),
          m_block_endpoints(m_block_maxs + 4 * m_blocks),
          m_blocks_data(m_block_endpoints + 4 * (m_blocks - 1)),
          m_universe(universe),
          m_block_codec(block_codec),
          m_block_size(block_codec->block_size()) {
        PISA_ASSERT_CONCEPT(
            (concepts::FrequencyPostingCursor<BlockInvertedIndexCursor>
             && concepts::SortedPostingCursor<BlockInvertedIndexCursor>)
        );

        if constexpr (profiling == Profiling::On) {
            m_profiler = block_profiler::open_list(term_id, m_blocks);
        }

        m_docs_buf.resize(m_block_size);
        m_freqs_buf.resize(m_block_size);
        reset();
    }

    void reset() { decode_docs_block(0); }

    void PISA_ALWAYSINLINE next() {
        ++m_pos_in_block;
        if PISA_UNLIKELY (m_pos_in_block == m_cur_block_size) {
            if (m_cur_block + 1 == m_blocks) {
                m_cur_docid = m_universe;
                return;
            }
            decode_docs_block(m_cur_block + 1);
        } else {
            m_cur_docid += m_docs_buf[m_pos_in_block] + 1;
        }
    }

    /**
     * Moves to the next document, counting from the current position,
     * with the ID equal to or greater than `lower_bound`.
     *
     * In particular, if called with a value that is less than or equal
     * to the current document ID, the position will not change.
     */
    void PISA_ALWAYSINLINE next_geq(uint64_t lower_bound) {
        if PISA_UNLIKELY (lower_bound > m_cur_block_max) {
            // binary search seems to perform worse here
            if (lower_bound > block_max(m_blocks - 1)) {
                m_cur_docid = m_universe;
                return;
            }

            uint64_t block = m_cur_block + 1;
            while (block_max(block) < lower_bound) {
                ++block;
            }

            decode_docs_block(block);
        }

        while (docid() < lower_bound) {
            m_cur_docid += m_docs_buf[++m_pos_in_block] + 1;
            assert(m_pos_in_block < m_cur_block_size);
        }
    }

    void PISA_ALWAYSINLINE move(uint64_t pos) {
        assert(pos >= position());
        uint64_t block = pos / m_block_size;
        if PISA_UNLIKELY (block != m_cur_block) {
            decode_docs_block(block);
        }
        while (position() < pos) {
            m_cur_docid += m_docs_buf[++m_pos_in_block] + 1;
        }
    }

    uint64_t docid() const { return m_cur_docid; }

    uint64_t PISA_ALWAYSINLINE freq() {
        if (!m_freqs_decoded) {
            decode_freqs_block();
        }
        return m_freqs_buf[m_pos_in_block] + 1;
    }

    uint64_t PISA_ALWAYSINLINE value() { return freq(); }

    uint64_t position() const { return m_cur_block * m_block_size + m_pos_in_block; }

    uint64_t size() const noexcept { return m_n; }

    uint64_t num_blocks() const { return m_blocks; }

    uint64_t stats_freqs_size() const {
        // XXX rewrite in terms of get_blocks()
        uint64_t bytes = 0;
        uint8_t const* ptr = m_blocks_data;
        static const uint64_t block_size = m_block_size;
        std::vector<uint32_t> buf(block_size);
        for (size_t b = 0; b < m_blocks; ++b) {
            uint32_t cur_block_size =
                ((b + 1) * block_size <= size()) ? block_size : (size() % block_size);

            uint32_t cur_base = (b != 0U ? block_max(b - 1) : uint32_t(-1)) + 1;
            uint8_t const* freq_ptr = m_block_codec->decode(
                ptr, buf.data(), block_max(b) - cur_base - (cur_block_size - 1), cur_block_size
            );
            ptr = m_block_codec->decode(freq_ptr, buf.data(), uint32_t(-1), cur_block_size);
            bytes += ptr - freq_ptr;
        }

        return bytes;
    }

    struct block_data {
        uint32_t index;
        uint32_t max;
        uint32_t size;
        uint32_t doc_gaps_universe;
        uint8_t const* docs_begin;
        uint8_t const* freqs_begin;
        uint8_t const* end;
        BlockCodec const* block_codec;

        void append_docs_block(std::vector<uint8_t>& out) const {
            out.insert(out.end(), docs_begin, freqs_begin);
        }

        void append_freqs_block(std::vector<uint8_t>& out) const {
            out.insert(out.end(), freqs_begin, end);
        }

        void decode_doc_gaps(std::vector<uint32_t>& out) const {
            out.resize(size);
            block_codec->decode(docs_begin, out.data(), doc_gaps_universe, size);
        }

        void decode_freqs(std::vector<uint32_t>& out) const {
            out.resize(size);
            block_codec->decode(freqs_begin, out.data(), uint32_t(-1), size);
        }
    };

    std::vector<block_data> get_blocks() {
        std::vector<block_data> blocks;

        uint8_t const* ptr = m_blocks_data;
        static const uint64_t block_size = m_block_size;
        std::vector<uint32_t> buf(block_size);
        for (size_t b = 0; b < m_blocks; ++b) {
            blocks.emplace_back();
            uint32_t cur_block_size =
                ((b + 1) * block_size <= size()) ? block_size : (size() % block_size);

            uint32_t cur_base = (b != 0U ? block_max(b - 1) : uint32_t(-1)) + 1;
            uint32_t gaps_universe = block_max(b) - cur_base - (cur_block_size - 1);

            blocks.back().index = b;
            blocks.back().size = cur_block_size;
            blocks.back().docs_begin = ptr;
            blocks.back().doc_gaps_universe = gaps_universe;
            blocks.back().max = block_max(b);
            blocks.back().block_codec = m_block_codec;

            uint8_t const* freq_ptr =
                m_block_codec->decode(ptr, buf.data(), gaps_universe, cur_block_size);
            blocks.back().freqs_begin = freq_ptr;
            ptr = m_block_codec->decode(freq_ptr, buf.data(), uint32_t(-1), cur_block_size);
            blocks.back().end = ptr;
        }

        assert(blocks.size() == num_blocks());
        return blocks;
    }

  private:
    uint32_t block_max(uint32_t block) const { return ((uint32_t const*)m_block_maxs)[block]; }

    void PISA_NOINLINE decode_docs_block(uint64_t block) {
        static const uint64_t block_size = m_block_size;
        uint32_t endpoint = block != 0U ? ((uint32_t const*)m_block_endpoints)[block - 1] : 0;
        uint8_t const* block_data = m_blocks_data + endpoint;
        m_cur_block_size = ((block + 1) * block_size <= size()) ? block_size : (size() % block_size);
        uint32_t cur_base = (block != 0U ? block_max(block - 1) : uint32_t(-1)) + 1;
        m_cur_block_max = block_max(block);
        m_freqs_block_data = m_block_codec->decode(
            block_data, m_docs_buf.data(), m_cur_block_max - cur_base - (m_cur_block_size - 1), m_cur_block_size
        );
        intrinsics::prefetch(m_freqs_block_data);

        m_docs_buf[0] += cur_base;

        m_cur_block = block;
        m_pos_in_block = 0;
        m_cur_docid = m_docs_buf[0];
        m_freqs_decoded = false;

        if constexpr (profiling == Profiling::On) {
            ++m_profiler[2 * m_cur_block];
        }
    }

    void PISA_NOINLINE decode_freqs_block() {
        uint8_t const* next_block = m_block_codec->decode(
            m_freqs_block_data, m_freqs_buf.data(), uint32_t(-1), m_cur_block_size
        );
        intrinsics::prefetch(next_block);
        m_freqs_decoded = true;

        if constexpr (profiling == Profiling::On) {
            ++m_profiler[2 * m_cur_block + 1];
        }
    }

    uint32_t m_n{0};
    uint8_t const* m_base;
    uint32_t m_blocks;
    uint8_t const* m_block_maxs;
    uint8_t const* m_block_endpoints;
    uint8_t const* m_blocks_data;
    uint64_t m_universe;

    uint32_t m_cur_block{0};
    uint32_t m_pos_in_block{0};
    uint32_t m_cur_block_max{0};
    uint32_t m_cur_block_size{0};
    uint32_t m_cur_docid{0};

    uint8_t const* m_freqs_block_data{nullptr};
    bool m_freqs_decoded{false};

    std::vector<uint32_t> m_docs_buf;
    std::vector<uint32_t> m_freqs_buf;
    BlockCodec const* m_block_codec;
    std::size_t m_block_size;
    block_profiler::counter_type* m_profiler = nullptr;
};

struct SizeStats {
    mapper::size_node_ptr size_tree = nullptr;
    std::size_t docs = 0;
    std::size_t freqs = 0;
};

class BlockInvertedIndex {
    global_parameters m_params;
    std::size_t m_size{0};
    std::size_t m_num_docs{0};
    bit_vector m_endpoints;
    mapper::mappable_vector<std::uint8_t> m_lists;
    MemorySource m_source;
    BlockCodecPtr m_block_codec;

  protected:
    void check_term_range(std::size_t term_id) const;

    friend class index::block::InMemoryPostingAccumulator;
    friend class index::block::StreamPostingAccumulator;

    explicit BlockInvertedIndex(BlockCodecPtr block_codec);

  public:
    using document_enumerator = BlockInvertedIndexCursor<>;

    BlockInvertedIndex(MemorySource source, BlockCodecPtr block_codec);

    template <typename Visitor>
    void map(Visitor& visit) {
        visit(m_params, "m_params")(m_size, "m_size")(m_num_docs, "m_num_docs")(
            m_endpoints, "m_endpoints")(m_lists, "m_lists");
    }

    [[nodiscard]] auto operator[](std::size_t term_id) const -> BlockInvertedIndexCursor<>;

    /**
     * The size of the index, i.e., the number of terms (posting lists).
     */
    [[nodiscard]] auto size() const noexcept -> std::size_t { return m_size; }

    /**
     * The number of distinct documents in the index.
     */
    [[nodiscard]] auto num_docs() const noexcept -> std::uint64_t { return m_num_docs; }

    void warmup(std::size_t term_id) const;

    [[nodiscard]] auto size_stats() -> SizeStats;
};

class ProfilingBlockInvertedIndex: public BlockInvertedIndex {
  public:
    using document_enumerator = BlockInvertedIndexCursor<Profiling::On>;

    ProfilingBlockInvertedIndex(MemorySource source, BlockCodecPtr block_codec);

    [[nodiscard]] auto operator[](std::size_t term_id
    ) const -> BlockInvertedIndexCursor<Profiling::On>;
};

namespace index::block {

    class PostingAccumulator {
      protected:
        BlockCodecPtr m_block_codec;
        std::size_t m_num_docs;
        std::string m_output_filename;
        bool m_finished = false;

      public:
        explicit PostingAccumulator(
            BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
        );

        virtual void accumulate_posting_list(
            std::size_t n, std::uint32_t const* docs, std::uint32_t const* freqs
        ) = 0;

        virtual void finish() = 0;

        void write(
            std::vector<uint8_t>& out,
            std::uint32_t n,
            std::uint32_t const* docs,
            std::uint32_t const* freqs
        );
    };

    class InMemoryPostingAccumulator: public PostingAccumulator {
        global_parameters m_params{};
        std::vector<std::uint64_t> m_endpoints{};
        std::vector<std::uint8_t> m_lists{};

      public:
        explicit InMemoryPostingAccumulator(
            BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
        );

        void accumulate_posting_list(
            std::uint64_t n, std::uint32_t const* docs, std::uint32_t const* freqs
        ) override;

        void finish() override;
    };

    class StreamPostingAccumulator: public PostingAccumulator {
        TemporaryDirectory m_tmp{};
        std::filesystem::path m_tmp_file;
        std::ofstream m_postings_output;
        std::vector<std::uint64_t> m_endpoints{0};
        std::size_t m_postings_bytes_written{0};
        global_parameters m_params{};

      public:
        explicit StreamPostingAccumulator(
            BlockCodecPtr block_codec, std::size_t num_docs, std::string output_filename
        );

        void accumulate_posting_list(
            std::uint64_t n, std::uint32_t const* docs, std::uint32_t const* freqs
        ) override;

        void finish() override;
    };

};  // namespace index::block

class BlockIndexBuilder {
  protected:
    // binary_freq_collection m_input;
    BlockCodecPtr m_block_codec;
    ScorerParams m_scorer_params;
    std::optional<QuantizingScorer> m_quantizing_scorer;
    bool m_check = false;
    bool m_in_memory = false;

    auto resolve_accumulator(std::size_t num_docs, std::string const& index_path)
        -> std::unique_ptr<index::block::PostingAccumulator>;

  public:
    BlockIndexBuilder(BlockCodecPtr block_codec, ScorerParams scorer_params);
    auto check(bool check) -> BlockIndexBuilder&;
    auto in_memory(bool in_mem) -> BlockIndexBuilder&;

    template <typename WandData>
    auto quantize(Size bits, WandData const& wdata) -> BlockIndexBuilder& {
        LinearQuantizer quantizer(wdata.index_max_term_weight(), bits.as_int());
        m_quantizing_scorer.emplace(scorer::from_params(m_scorer_params, wdata), quantizer);
        return *this;
    }

    template <typename Documents, typename Frequencies>
    void accumulate_posting_list(
        Documents const& documents,
        Frequencies const& frequencies,
        std::uint32_t term_id,
        index::block::PostingAccumulator* accumulator
    ) {
        std::size_t size = documents.size();
        if (m_quantizing_scorer.has_value()) {
            auto term_scorer = m_quantizing_scorer->term_scorer(term_id);
            std::vector<std::uint32_t> quants;
            for (size_t pos = 0; pos < size; ++pos) {
                std::uint32_t doc = *(documents.begin() + pos);
                std::uint32_t freq = *(frequencies.begin() + pos);
                std::uint32_t quant_score = term_scorer(doc, freq);
                quants.push_back(quant_score);
            }
            assert(quants.size() == size);
            accumulator->accumulate_posting_list(size, documents.begin(), &quants[0]);
        } else {
            accumulator->accumulate_posting_list(size, documents.begin(), frequencies.begin());
        }
    }

    void build(binary_freq_collection const& input, std::string const& index_path);
};

};  // namespace pisa
