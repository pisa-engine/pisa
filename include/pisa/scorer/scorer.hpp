#pragma once

#include <string>

#include "bm25.hpp"
#include "dph.hpp"
#include "index_scorer.hpp"
#include "pl2.hpp"
#include "qld.hpp"
#include "quantized.hpp"
#include "spdlog/spdlog.h"

#define PISA_WITH_SCORER_TYPE(Scorer, scorer_name, WandData, body) \
    {                                                              \
        if (scorer_name == "bm25") {                               \
            using Scorer = bm25<WandData>;                         \
            body                                                   \
        } else if (scorer_name == "qld") {                         \
            using Scorer = qld<WandData>;                          \
            body                                                   \
        } else if (scorer_name == "pl2") {                         \
            using Scorer = pl2<WandData>;                          \
            body                                                   \
        } else if (scorer_name == "dph") {                         \
            using Scorer = dph<WandData>;                          \
            body                                                   \
        } else if (scorer_name == "quantized") {                   \
            using Scorer = quantized<WandData>;                    \
            body                                                   \
        } else {                                                   \
            spdlog::error("Unknown scorer {}", scorer_name);       \
            std::abort();                                          \
        }                                                          \
    }

