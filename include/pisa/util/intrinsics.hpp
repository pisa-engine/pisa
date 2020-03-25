#pragma once

#include <cstdint>
#include <x86intrin.h>
#if defined(__SSE4_2__)
    #define USE_POPCNT 1
#else
    #define USE_POPCNT 0
#endif

#if defined(__GNUC__) || defined(__clang__)
    #define __INTRIN_INLINE inline __attribute__((__always_inline__))
#elif defined(_MSC_VER)
    #define __INTRIN_INLINE inline __forceinline
#else
    #define __INTRIN_INLINE inline
#endif

namespace pisa { namespace intrinsics {

    __INTRIN_INLINE uint64_t byteswap64(uint64_t value)
    {
#if defined(__GNUC__) || defined(__clang__)
        return __builtin_bswap64(value);
#elif defined(_MSC_VER)
        return _byteswap_uint64(value);
#else
    #error Unsupported platform
#endif
    }

    __INTRIN_INLINE bool bsf64(unsigned long* const index, const uint64_t mask)
    {
#if defined(__GNUC__) || defined(__clang__)
        if (mask != 0U) {
            *index = (unsigned long)__builtin_ctzll(mask);
            return true;
        }
        return false;

#elif defined(_MSC_VER)
        return _BitScanForward64(index, mask) != 0;
#else
    #error Unsupported platform
#endif
    }

    __INTRIN_INLINE bool bsr64(unsigned long* const index, const uint64_t mask)
    {
#if defined(__GNUC__) || defined(__clang__)
        if (mask != 0U) {
            *index = (unsigned long)(63 - __builtin_clzll(mask));
            return true;
        }
        return false;
#elif defined(_MSC_VER)
        return _BitScanReverse64(index, mask) != 0;
#else
    #error Unsupported platform
#endif
    }

    template <typename T>
    __INTRIN_INLINE void prefetch(T const* ptr)
    {
#if defined(__SSE__)
        _mm_prefetch((const char*)ptr, _MM_HINT_T0);
#endif
    }

#if USE_POPCNT

    __INTRIN_INLINE uint64_t popcount(uint64_t x) { return uint64_t(_mm_popcnt_u64(x)); }

#endif /* USE_POPCNT */

}}  // namespace pisa::intrinsics
