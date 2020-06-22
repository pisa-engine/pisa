#pragma once

#include "bit_vector.hpp"
#include <immintrin.h>
namespace pisa {

bit_vector compute_live_quant16(std::vector<std::vector<uint16_t>> const& scores, uint16_t threshold)
{
    bit_vector_builder bv;
    bv.reserve(scores[0].size());
    for (size_t i = 0; i < scores[0].size(); i += 1) {
        uint16_t sum = scores[0][i];
        for (size_t term = 1; term < scores.size(); ++term) {
            sum += scores[term][i];
        }
        bv.append_bits(static_cast<uint64_t>(sum >= threshold), 1);
    }
    return bit_vector(&bv);
}

#ifdef __AVX__

bit_vector avx_compute_live_quant16(std::vector<std::vector<uint16_t>> const& scores, uint16_t threshold)
{
    bit_vector_builder bv;
    bv.reserve(scores[0].size());
    __m128i thresholds = _mm_set1_epi16(threshold);
    size_t i = 0;
    for (; i < scores[0].size() and scores[0].size() - i > 0; i += 8) {
        __m128i sum = _mm_loadu_si128((__m128i*)(scores[0].data() + i));
        for (size_t term = 1; term < scores.size(); ++term) {
            sum = _mm_adds_epu16(sum, _mm_loadu_si128((__m128i*)(scores[term].data() + i)));
        }

        __m128i masksL1 = _mm_cmpeq_epi16(_mm_max_epu16(sum, thresholds), sum);
        __m128i lensAll = _mm_shuffle_epi8(
            masksL1, _mm_setr_epi8(0, 2, 4, 6, 8, 10, 12, 14, -1, -1, -1, -1, -1, -1, -1, -1));
        uint32_t maskBitsL1 = _mm_movemask_epi8(lensAll);
        bv.append_bits(maskBitsL1, 8);
    }

    auto remain = scores[0].size() - i;
    if (remain > 0) {
        uint32_t mask = 0;
        for (; i < scores[0].size(); ++i) {
            uint16_t sum = scores[0][i];
            for (size_t term = 1; term < scores.size(); ++term) {
                sum += scores[term][i];
            }
            if (sum >= threshold) {
                mask += 1;
                mask = (mask << 1);
            }
        }
        bv.append_bits(mask, remain);
    }
    return bit_vector(&bv);
}

#endif

#ifdef __AVX2__

bit_vector
avx2_compute_live_quant16(std::vector<std::vector<uint16_t>> const& scores, uint16_t threshold)
{
    bit_vector_builder bv;
    bv.reserve(scores[0].size());
    __m256i thresholds = _mm256_set1_epi16(threshold);
    size_t i = 0;
    for (; i < scores[0].size() and scores[0].size() - i > 0; i += 16) {
        __m256i sum = _mm256_loadu_si256((__m256i*)(scores[0].data() + i));
        for (size_t term = 1; term < scores.size(); ++term) {
            sum = _mm256_adds_epu16(sum, _mm256_loadu_si256((__m256i*)(scores[term].data() + i)));
        }
        __m256i masksL1 = _mm256_cmpeq_epi16(_mm256_max_epu16(sum, thresholds), sum);
        uint32_t maskBitsL1 = _mm_movemask_epi8(_mm_packs_epi16(
            _mm256_extractf128_si256(masksL1, 0), _mm256_extractf128_si256(masksL1, 1)));
        bv.append_bits(maskBitsL1, 16);
    }

    auto remain = scores[0].size() - i;
    if (remain > 0) {
        uint32_t mask = 0;
        for (; i < scores[0].size(); ++i) {
            uint16_t sum = scores[0][i];
            for (size_t term = 1; term < scores.size(); ++term) {
                sum += scores[term][i];
            }
            if (sum >= threshold) {
                mask += 1;
                mask = (mask << 1);
            }
        }
        bv.append_bits(mask << (32 - remain), remain);
    }
    return bit_vector(&bv);
}

#endif

}  // namespace pisa
