#pragma once

// noinline
#ifdef _MSC_VER
    #define PISA_NOINLINE __declspec(noinline)
#elif defined(__clang__) || defined(__GNUC__)
    #define PISA_NOINLINE __attribute__((__noinline__))
#else
    #define PISA_NOINLINE
#endif

// always inline
#ifdef _MSC_VER
    #define PISA_ALWAYSINLINE __forceinline
#elif defined(__clang__) || defined(__GNUC__)
    #define PISA_ALWAYSINLINE inline __attribute__((__always_inline__))
#else
    #define PISA_ALWAYSINLINE inline
#endif

// flatten
#if defined(__GNUC__) && !defined(__clang__)
    #define PISA_FLATTEN_FUNC __attribute__((always_inline, flatten))
#else
    #define PISA_FLATTEN_FUNC PISA_ALWAYSINLINE
#endif