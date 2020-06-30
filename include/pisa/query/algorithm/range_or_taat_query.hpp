#pragma once

#include "query/queries.hpp"
#include "topk_queue.hpp"
#include "util/do_not_optimize_away.hpp"
#include "wand_data_range.hpp"

#include "util/simdprune_tables.hpp"
#include <range/v3/algorithm/partial_sort.hpp>

namespace pisa {

// table modified and copied from
// http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetTable
static const unsigned char BitsSetTable256mul2[256] = {
#define B2(n) (n) * 2, (n)*2 + 2, (n)*2 + 2, (n)*2 + 4
#define B4(n) B2(n), B2(n + 1), B2(n + 1), B2(n + 2)
#define B6(n) B4(n), B4(n + 1), B4(n + 1), B4(n + 2)
    B6(0), B6(1), B6(1), B6(2)
#undef B2
#undef B4
#undef B6
};
static inline __m128i prune_epi16(__m128i x, int mask)
{
    return _mm_shuffle_epi8(x, _mm_loadu_si128((const __m128i*)mask128_epi16 + mask));
}

static inline __m128i skinnyprune_epi8(__m128i x, int mask)
{
    int mask1 = mask & 0xFF;
    int mask2 = (mask >> 8) & 0xFF;
    __m128i shufmask = _mm_castps_si128(_mm_loadh_pi(
        _mm_castsi128_ps(_mm_loadl_epi64((const __m128i*)(thintable_epi8 + mask1))),
        (const __m64*)(thintable_epi8 + mask2)));
    shufmask = _mm_add_epi8(shufmask, _mm_set_epi32(0x08080808, 0x08080808, 0, 0));
    __m128i pruned = _mm_shuffle_epi8(x, shufmask);
    intptr_t popx2 = BitsSetTable256mul2[mask1];
    __m128i compactmask = _mm_loadu_si128((const __m128i*)(pshufb_combine_table + popx2 * 8));
    return _mm_shuffle_epi8(pruned, compactmask);
}

static inline __m256i prune256_epi32(__m256i x, int mask)
{
    return _mm256_permutevar8x32_epi32(x, _mm256_loadu_si256((const __m256i*)mask256_epi32 + mask));
}

template <class T>
inline void Log(const __m128i& value)
{
    const size_t n = sizeof(__m128i) / sizeof(T);
    T buffer[n];
    _mm_storeu_si128((__m128i*)buffer, value);
    for (int i = 0; i < n; i++)
        std::cout << std::to_string(buffer[i]) << " ";
    std::cout << std::endl;
}

template <class T>
inline void Log256(const __m256i& value)
{
    const size_t n = sizeof(__m256i) / sizeof(T);
    T buffer[n];
    _mm256_storeu_si256((__m256i*)buffer, value);
    for (int i = 0; i < n; i++)
        std::cout << std::to_string(buffer[i]) << " ";
    std::cout << std::endl;
}

static uint32_t ids[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
                         16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};

// #define SCALAR
// #define AVX
#define AVX2
// #define AVX_512
#ifdef AVX2
template <size_t range_size>
void simd_aggregate(
    std::vector<uint16_t>& topk_vector,
    std::array<uint16_t, range_size> const& accumulator,
    uint16_t threshold,
    uint32_t min_docid,
    size_t& total)
{
    __m256i thresholds = _mm256_set1_epi16(threshold);
    __m256i min_docs = _mm256_set1_epi32(min_docid);

    for (size_t i = 0; i < accumulator.size(); i += 16) {
        __m256i acc = _mm256_loadu_si256((__m256i*)(accumulator.data() + i));
        // Log256<uint16_t>(acc);
        // Log256<uint16_t>(thresholds);
        __m256i masksL1 = _mm256_cmpeq_epi16(_mm256_max_epu16(acc, thresholds), acc);
        uint16_t maskBitsL1 = _mm_movemask_epi8(_mm_packs_epi16(
            _mm256_extractf128_si256(masksL1, 0), _mm256_extractf128_si256(masksL1, 1)));
        size_t count1 = broadword::popcount(uint8_t(maskBitsL1 >> 8));
        size_t count2 = broadword::popcount(uint8_t(maskBitsL1));
        // Log256<uint16_t>(masksL1);
        // std::cout << std::bitset<16>(  maskBitsL1) << std::endl;;
        // std::cout << count1 << " " << count2 << std::endl;
        if (count1 > 0) {
            // Log<uint16_t>(_mm256_extractf128_si256(acc,1));
            // std::cout << std::bitset<8>(  reverse((uint8_t(maskBitsL1 >> 8)))) << std::endl;;

            // Log<uint16_t>(_mm256_extractf128_si256(acc, 1));
            uint8_t mask =  ~((uint8_t(maskBitsL1 >> 8)));

            _mm_storeu_si128(
                (__m128i*)(topk_vector.data() + total), prune_epi16(_mm256_extractf128_si256(acc, 1), mask));
            total += count1;
        }
        if (count2 > 0) {
            // Log<uint16_t>(_mm256_extractf128_si256(acc, 0));
            // Log<uint16_t>(prune_epi16(_mm256_extractf128_si256(acc, 0), reverse((uint8_t(maskBitsL1)))));
            // std::cout << std::bitset<8>(  reverse((uint8_t(maskBitsL1)))) << std::endl;;
            
            uint8_t mask =  ~((uint8_t(maskBitsL1)));
            _mm_storeu_si128(
                (__m128i*)(topk_vector.data() + total), prune_epi16(_mm256_extractf128_si256(acc, 0), mask));
            total += count2;
        }

        // Uncomment
        // __m128i docs = _mm_loadu_si128((__m128i*)(ids + i));
        // _mm_storeu_si128((__m128i*)(topdoc_vector.data() + total), docs);
        // docs = _mm_loadu_si128((__m128i*)(ids + 4 + i));
        // _mm_storeu_si128((__m128i*)(topdoc_vector.data() + total), docs);
        // docs = _mm_loadu_si128((__m128i*)(ids + 8 + i));
        // _mm_storeu_si128((__m128i*)(topdoc_vector.data() + total), docs);
        // docs = _mm_loadu_si128((__m128i*)(ids + 12 + i));
        // _mm_storeu_si128((__m128i*)(topdoc_vector.data() + total), docs);
        // Uncomment till here

        // topk_vector.insert(topk_vector.end(),buffer.begin(),buffer.begin() + count);
        // auto doc_count = broadword::popcount((maskBitsL1<<24)>>31);
        // if(doc_count){
        //     __m256i docs = _mm256_loadu_si256((__m256i *)(ids+i));
        //     // Log256<uint32_t>(docs);
        //     docs = _mm256_add_epi32(docs, min_docs);
        //     _mm256_storeu_si256((__m256i*)doc_buffer.data(), prune256_epi32(docs,
        //     ~((maskBitsL1<<24)>>31)));

        //     // Log256<uint32_t>(min_docs);
        //     // Log256<uint32_t>(docs);
        //     // Log<uint8_t>(masksL1);
        //     // std::cout << doc_count << std::endl;
        //     // std::cout << std::bitset<32>(maskBitsL1) << std::endl;
        //     // std::cout << std::bitset<32>((maskBitsL1<<24)>>31) << std::endl;

        //     // for(auto&& i : doc_buffer) {
        //     //     std::cout << i << std::endl;
        //     // }
        //     // std::abort();
        //     topdoc_vector.insert(topdoc_vector.end(),doc_buffer.begin(),doc_buffer.begin() +
        //     doc_count);
        // }
        // doc_count = broadword::popcount(maskBitsL1>>8);
        // if(doc_count){
        //     __m256i docs = _mm256_loadu_si256((__m256i *)(ids+i + 8));
        //     docs = _mm256_add_epi32(docs, min_docs);
        //     _mm256_storeu_si256((__m256i*)doc_buffer.data(), prune256_epi32(docs,
        //     ~(maskBitsL1>>8)));
        //     topdoc_vector.insert(topdoc_vector.end(),doc_buffer.begin(),doc_buffer.begin() +
        //     doc_count);
        // }
    }

    //     unsigned long pos_in_word=0;
    //     while (broadword::lsb(maskBitsL1, pos_in_word) != 0U) {
    //         maskBitsL1 = maskBitsL1 & (maskBitsL1 - 1);  // clear LSB
    //         pos_in_word = (pos_in_word & ~uint64_t(31)) + pos_in_word;
    //         topk_vector.push_back(pos_in_word+min_docid);

    //     }
}
#endif
#ifdef AVX_512
void simd_aggregate(
    std::vector<uint8_t>& topk_vector,
    std::vector<uint32_t>& topdoc_vector,
    std::array<uint8_t, 32>& accumulator,
    uint8_t threshold,
    uint32_t min_docid,
    size_t& total)
{
    __m512i thresholds = _mm512_set1_epi8(threshold);
    // __m256i min_docs = _mm256_set1_epi32(min_docid);
    static std::array<uint8_t, 32> buffer;
    // std::array<uint32_t, 8> doc_buffer;

    __m256i acc = _mm256_loadu_si256((__m256i*)(accumulator.data()));
    uint64_t maskBitsL1 = _mm256_cmpeq_epi8_mask(_mm256_max_epu8(acc, thresholds), acc);
    size_t count = broadword::popcount(maskBitsL1);
    if (count > 0) {
        _mm_storeu_si128((__m128i*)buffer.data(), skinnyprune_epi8(acc, ~maskBitsL1));

        // _mm512_storeu_si512((__m512i*)buffer.data(), _mm512_maskz_compress_epi8());
        topk_vector.insert(topk_vector.end(), buffer.begin(), buffer.begin() + count);
        total += count;
    }
}
#endif

template <size_t range_size>
struct range_or_taat_query {
    explicit range_or_taat_query(topk_queue& topk) : m_topk(topk) {}

    template <typename CursorRange>
    void operator()(CursorRange&& cursors, uint64_t max_docid, bit_vector const& live_blocks, std::vector<uint16_t>& topk_vector, std::vector<uint32_t>& topdoc_vector)
    {
        if (cursors.empty()) {
            return;
        }

        size_t total = 0;
        bit_vector::unary_enumerator en(live_blocks, 0);
        size_t i = en.next(), end = (i + 1) * range_size;
        for (; i < live_blocks.size() and end < max_docid; i = en.next(), end = (i + 1) * range_size) {
            auto min_docid = end - range_size;
            std::array<uint16_t, range_size> addon{};

            for (auto&& c: cursors) {
                c.next_geq(min_docid);
                while (c.docid() < end) {
                    addon[c.docid() % range_size] += c.freq();
                    c.next();
                }
            }
            // for (int j = 0; j < range_size; ++j) {
            //     m_topk.insert(addon[j], i * range_size + j);
            // }

            simd_aggregate<range_size>(topk_vector, addon, m_topk.threshold(), min_docid, total);
        }

        size_t kk = std::min(size_t(m_topk.capacity()), total);
        auto zip = ranges::views::zip(topk_vector, topdoc_vector);
        ranges::partial_sort(
            zip.begin(),
            zip.begin() + kk,
            zip.begin() + total,
            [](const auto& lhs, const auto& rhs) -> decltype(auto) {
                return std::get<0>(lhs) > std::get<0>(rhs);
            });
          topk_vector.resize(kk);
          topk_vector.shrink_to_fit();
    }

    std::vector<std::pair<float, uint64_t>> const& topk() const { return m_topk.topk(); }

  private:
    topk_queue& m_topk;
};

}  // namespace pisa