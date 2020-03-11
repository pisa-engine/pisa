#pragma once

//  Likeliness annotations
#if defined(__GNUC__)
    #define PISA_LIKELY(x) (__builtin_expect(!!(x), 1))
    #define PISA_UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
    #define PISA_LIKELY(x) (x)
    #define PISA_UNLIKELY(x) (x)
#endif
