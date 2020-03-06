#pragma once

#include <iostream>
#include <stack>
#include <stdint.h>
#include <vector>

#define _STRINGIZE_I(x) #x
#define _STRINGIZE(x) _STRINGIZE_I(x)

#define MY_REQUIRE_EQUAL(A, B, MSG)                                                                \
    CHECKED_ELSE((A) == (B))                                                                       \
    {                                                                                              \
        FAIL(_STRINGIZE(A) << " == " << _STRINGIZE(B) << " [" << A << " != " << B << "] " << MSG); \
    }
inline std::vector<bool> random_bit_vector(size_t n = 10000, double density = 0.5)
{
    std::vector<bool> v;
    for (size_t i = 0; i < n; ++i) {
        v.push_back(rand() < (RAND_MAX * density));
    }
    return v;
}
