#pragma once

//  Likeliness annotations
#if defined(PISA_CXX20)
    #define PISA_LIKELY(x) (x) [[likely]]
    #define PISA_UNLIKELY(x) (x) [[unlikely]]
#elif defined(__GNUC__)
    #define PISA_LIKELY(x) (__builtin_expect(!!(x), 1))
    #define PISA_UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
    #define PISA_LIKELY(x) (x)
    #define PISA_UNLIKELY(x) (x)
#endif
