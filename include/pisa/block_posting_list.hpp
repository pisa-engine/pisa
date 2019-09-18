#pragma once

#include <vector>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "codec/block_codecs.hpp"
#include "codec/list.hpp"
#include "util/util.hpp"

namespace pisa {

template <typename BlockCodec>
struct BlockPostingCursor {

    struct MutableState {
        std::uint32_t block = 0;
        std::uint32_t pos_in_block = 0;
        std::uint32_t max_in_block = 0;
        std::uint32_t block_size = 0;
        std::uint32_t docid = 0;
        bool frequencies_decoded = false;
    };

    struct BlockData {
       public:
        void append_docs_block(std::vector<std::uint8_t> &out) const;
        void append_freqs_block(std::vector<std::uint8_t> &out) const;
        void decode_doc_gaps(std::vector<std::uint32_t> &out) const;
        void decode_freqs(std::vector<std::uint32_t> &out) const;

        std::uint32_t index;
        std::uint32_t max;
        std::uint32_t size;
        std::uint32_t doc_gaps_universe;
       private:
        friend struct BlockPostingCursor;

        std::uint8_t const *docs_begin;
        std::uint8_t const *freqs_begin;
        std::uint8_t const *end;
    };

    [[nodiscard]] static auto from(std::uint8_t const *data, uint64_t universe)
        -> BlockPostingCursor;

    void reset();
    void next();
    void next_geq(std::uint64_t docid);
    void move(std::uint64_t position);

    std::uint64_t docid() const;
    std::uint64_t freq();
    std::uint64_t position() const;
    std::uint64_t size() const;
    std::uint64_t num_blocks() const;
    std::uint64_t stats_freqs_size() const;
    std::vector<BlockData> get_blocks();

   private:
    std::uint32_t block_max(std::uint32_t block) const;
    void decode_docs_block(std::uint64_t block);
    void decode_freqs_block();

    std::uint32_t m_length;
    std::uint32_t m_num_blocks;
    std::uint64_t m_universe;

    std::uint8_t const *m_block_maxima;
    std::uint8_t const *m_block_endpoints;
    std::uint8_t const *m_document_data;
    std::uint8_t const *m_frequency_data;

    MutableState m_state;

    std::vector<std::uint32_t> m_document_buf;
    std::vector<std::uint32_t> m_frequency_buf;
};

template <typename BlockCodec>
[[nodiscard]] auto BlockPostingCursor<BlockCodec>::from(std::uint8_t const *data, uint64_t universe)
    -> BlockPostingCursor
{
    BlockPostingCursor cursor;
    auto begin = TightVariableByte::decode(data, &cursor.m_length, 1);
    cursor.m_num_blocks = ceil_div(cursor.m_length, BlockCodec::block_size);
    cursor.m_block_maxima = begin;
    cursor.m_block_endpoints = std::next(cursor.m_block_maxima, 4 * cursor.m_num_blocks);
    cursor.m_document_data = std::next(cursor.m_block_endpoints, 4 * (cursor.m_num_blocks - 1));
    cursor.m_universe = universe;
    cursor.m_document_buf.resize(BlockCodec::block_size);
    cursor.m_frequency_buf.resize(BlockCodec::block_size);
    cursor.reset();
    return cursor;
}

template <typename BlockCodec>
void BlockPostingCursor<BlockCodec>::reset()
{
    decode_docs_block(0);
}

template <typename BlockCodec>
std::uint32_t BlockPostingCursor<BlockCodec>::block_max(std::uint32_t block) const
{
    return reinterpret_cast<std::uint32_t const *>(m_block_maxima)[block];
}

template <typename BlockCodec>
void PISA_NOINLINE BlockPostingCursor<BlockCodec>::decode_docs_block(std::uint64_t block)
{
    static constexpr std::uint64_t block_size = BlockCodec::block_size;
    std::uint32_t endpoint =
        block > 0 ? reinterpret_cast<uint32_t const *>(m_block_endpoints)[block - 1] : 0;
    std::uint8_t const *block_data = std::next(m_document_data, endpoint);
    m_state.block_size = ((block + 1) * block_size <= size()) ? block_size : (size() % block_size);
    std::uint32_t docid_offset = block ? block_max(block - 1) + 1 : 0;
    m_state.max_in_block = block_max(block);
    auto sum = m_state.max_in_block - docid_offset - (m_state.block_size - 1);
    m_frequency_data = BlockCodec::decode(block_data, &m_document_buf[0], sum, m_state.block_size);
    intrinsics::prefetch(m_frequency_data);
    m_document_buf[0] += docid_offset;

    m_state.block = block;
    m_state.pos_in_block = 0;
    m_state.docid = m_document_buf[0];
    m_state.frequencies_decoded = false;
}

template <typename BlockCodec>
void PISA_NOINLINE BlockPostingCursor<BlockCodec>::decode_freqs_block()
{
    auto next_block = BlockCodec::decode(
        m_frequency_data, &m_frequency_buf[0], std::uint32_t(-1), m_state.block_size);
    intrinsics::prefetch(next_block);
    m_state.frequencies_decoded = true;
}

template <typename BlockCodec>
void PISA_ALWAYSINLINE BlockPostingCursor<BlockCodec>::next()
{
    m_state.pos_in_block += 1;
    if (PISA_UNLIKELY(m_state.pos_in_block == m_state.block_size)) {
        if (m_state.block + 1 == m_num_blocks) {
            m_state.docid = m_universe;
            return;
        }
        decode_docs_block(m_state.block + 1);
    } else {
        m_state.docid += m_document_buf[m_state.pos_in_block] + 1;
    }
}

template <typename BlockCodec>
void PISA_ALWAYSINLINE BlockPostingCursor<BlockCodec>::next_geq(std::uint64_t next_docid)
{
    if (PISA_UNLIKELY(next_docid > m_state.max_in_block)) {
        if (next_docid > block_max(m_num_blocks - 1)) { // binary search seems to perform worse here
            m_state.docid = m_universe;
            return;
        }
        auto block = m_state.block + 1;
        while (block_max(block) < next_docid) {
            ++block;
        }
        decode_docs_block(block);
    }
    while (docid() < next_docid) {
        m_state.docid += m_document_buf[++m_state.pos_in_block] + 1;
        assert(m_state.pos_in_block < m_state.block_size);
    }
}

template <typename BlockCodec>
void PISA_ALWAYSINLINE BlockPostingCursor<BlockCodec>::move(std::uint64_t pos)
{
    assert(pos >= position());
    std::uint64_t block = pos / BlockCodec::block_size;
    if (PISA_UNLIKELY(block != m_state.block)) {
        decode_docs_block(block);
    }
    while (position() < pos) {
        m_state.docid += m_document_buf[++m_state.pos_in_block] + 1;
    }
}

template <typename BlockCodec>
inline std::uint64_t BlockPostingCursor<BlockCodec>::docid() const
{
    return m_state.docid;
}

template <typename BlockCodec>
std::uint64_t PISA_ALWAYSINLINE BlockPostingCursor<BlockCodec>::freq()
{
    if (!m_state.frequencies_decoded) {
        decode_freqs_block();
    }
    return m_frequency_buf[m_state.pos_in_block] + 1;
}

template <typename BlockCodec>
inline std::uint64_t BlockPostingCursor<BlockCodec>::position() const
{
    return m_state.block * BlockCodec::block_size + m_state.pos_in_block;
}

template <typename BlockCodec>
inline std::uint64_t BlockPostingCursor<BlockCodec>::size() const
{
    return m_length;
}

template <typename BlockCodec>
inline std::uint64_t BlockPostingCursor<BlockCodec>::num_blocks() const
{
    return m_num_blocks;
}

template <typename BlockCodec>
std::uint64_t BlockPostingCursor<BlockCodec>::stats_freqs_size() const
{
    std::uint64_t bytes = 0;
    auto ptr = m_document_data;
    static constexpr std::uint64_t block_size = BlockCodec::block_size;
    std::vector<std::uint32_t> buf(block_size);
    for (std::size_t block = 0; block < m_num_blocks; ++block) {
        std::uint32_t cur_block_size =
            (block + 1) * block_size <= size() ? block_size : (size() % block_size);
        std::uint32_t docid_offset = block ? block_max(block - 1) + 1 : 0;
        auto sum = block_max(block) - docid_offset - (cur_block_size - 1);
        auto freq_ptr = BlockCodec::decode(ptr, buf.data(), sum, cur_block_size);
        ptr = BlockCodec::decode(freq_ptr, buf.data(), std::uint32_t(-1), cur_block_size);
        bytes += ptr - freq_ptr;
    }
    return bytes;
}

template <typename BlockCodec>
std::vector<typename BlockPostingCursor<BlockCodec>::BlockData>
BlockPostingCursor<BlockCodec>::get_blocks()
{
    std::vector<BlockPostingCursor<BlockCodec>::BlockData> blocks;
    auto ptr = m_document_data;
    static constexpr std::uint64_t block_size = BlockCodec::block_size;
    std::vector<std::uint32_t> buf(block_size);
    for (std::size_t block = 0; block < m_num_blocks; ++block) {
        blocks.emplace_back();
        std::uint32_t cur_block_size =
            ((block + 1) * block_size <= size()) ? block_size : (size() % block_size);

        std::uint32_t docid_offset = block ? block_max(block - 1) + 1 : 0;
        std::uint32_t gaps_universe = block_max(block) - docid_offset - (cur_block_size - 1);

        blocks.back().index = block;
        blocks.back().size = cur_block_size;
        blocks.back().docs_begin = ptr;
        blocks.back().doc_gaps_universe = gaps_universe;
        blocks.back().max = block_max(block);

        auto freq_ptr = BlockCodec::decode(ptr, buf.data(), gaps_universe, cur_block_size);
        blocks.back().freqs_begin = freq_ptr;
        ptr = BlockCodec::decode(freq_ptr, buf.data(), std::uint32_t(-1), cur_block_size);
        blocks.back().end = ptr;
    }
    assert(blocks.size() == num_blocks());
    return blocks;
}

template <typename BlockCodec>
inline void BlockPostingCursor<BlockCodec>::BlockData::append_docs_block(
    std::vector<std::uint8_t> &out) const
{
    out.insert(out.end(), docs_begin, freqs_begin);
}

template <typename BlockCodec>
inline void BlockPostingCursor<BlockCodec>::BlockData::append_freqs_block(
    std::vector<std::uint8_t> &out) const
{
    out.insert(out.end(), freqs_begin, end);
}

template <typename BlockCodec>
inline void BlockPostingCursor<BlockCodec>::BlockData::decode_doc_gaps(
    std::vector<std::uint32_t> &out) const
{
    out.resize(size);
    BlockCodec::decode(docs_begin, out.data(), doc_gaps_universe, size);
}

template <typename BlockCodec>
inline void BlockPostingCursor<BlockCodec>::BlockData::decode_freqs(std::vector<std::uint32_t> &out) const
{
    out.resize(size);
    BlockCodec::decode(freqs_begin, out.data(), std::uint32_t(-1), size);
}

template <typename BlockCodec, typename DocIterator, typename FreqIterator>
static void write_block_posting_list(std::vector<std::uint8_t> &out,
                                     std::uint32_t n,
                                     DocIterator docs_begin,
                                     FreqIterator freqs_begin)
{
    TightVariableByte::encode_single(n, out);

    std::uint64_t block_size = BlockCodec::block_size;
    std::uint64_t blocks = ceil_div(n, block_size);
    std::size_t begin_block_maxs = out.size();
    std::size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
    std::size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
    out.resize(begin_blocks);

    DocIterator docs_it(docs_begin);
    FreqIterator freqs_it(freqs_begin);
    std::vector<uint32_t> docs_buf(block_size);
    std::vector<uint32_t> freqs_buf(block_size);
    uint32_t last_doc(-1);
    uint32_t block_base = 0;
    for (size_t b = 0; b < blocks; ++b) {
        uint32_t cur_block_size = ((b + 1) * block_size <= n) ? block_size : (n % block_size);

        for (size_t i = 0; i < cur_block_size; ++i) {
            uint32_t doc(*docs_it++);
            docs_buf[i] = doc - last_doc - 1;
            last_doc = doc;

            freqs_buf[i] = *freqs_it++ - 1;
        }
        *((uint32_t *)&out[begin_block_maxs + 4 * b]) = last_doc;

        BlockCodec::encode(
            docs_buf.data(), last_doc - block_base - (cur_block_size - 1), cur_block_size, out);
        BlockCodec::encode(freqs_buf.data(), uint32_t(-1), cur_block_size, out);
        if (b != blocks - 1) {
            *((uint32_t *)&out[begin_block_endpoints + 4 * b]) = out.size() - begin_blocks;
        }
        block_base = last_doc + 1;
    }
}

template <typename BlockDataRange>
static void write_blocks(std::vector<uint8_t> &out, uint32_t n, BlockDataRange const &input_blocks)
{
    TightVariableByte::encode_single(n, out);
    assert(input_blocks.front().index == 0); // first block must remain first

    uint64_t blocks = input_blocks.size();
    size_t begin_block_maxs = out.size();
    size_t begin_block_endpoints = begin_block_maxs + 4 * blocks;
    size_t begin_blocks = begin_block_endpoints + 4 * (blocks - 1);
    out.resize(begin_blocks);

    for (auto const &block : input_blocks) {
        size_t b = block.index;
        // write endpoint
        if (b != 0) {
            *((uint32_t *)&out[begin_block_endpoints + 4 * (b - 1)]) = out.size() - begin_blocks;
        }

        // write max
        *((uint32_t *)&out[begin_block_maxs + 4 * b]) = block.max;

        // copy block
        block.append_docs_block(out);
        block.append_freqs_block(out);
    }
}

#define LOOP_BODY(R, DATA, T)                                                               \
    struct T;                                                                               \
    extern template BlockPostingCursor<T> BlockPostingCursor<T>::from(std::uint8_t const *, \
                                                                      std::uint64_t);       \
    extern template void BlockPostingCursor<T>::decode_docs_block(std::uint64_t);           \
    extern template void BlockPostingCursor<T>::decode_freqs_block();                       \
    extern template std::uint64_t BlockPostingCursor<T>::stats_freqs_size() const;          \
    extern template std::vector<typename BlockPostingCursor<T>::BlockData>                  \
    BlockPostingCursor<T>::get_blocks();
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_BLOCK_CODEC_TYPES);
#undef LOOP_BODY

} // namespace pisa
