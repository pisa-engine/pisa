#pragma once

#ifdef __GNUC__
#ifndef __clang__
#if __GNUC__ > 9
#include <execution>
#else
#include <pstl/execution>
#endif
#else
#include <pstl/execution>
#endif
#else
#include <pstl/execution>
#endif
