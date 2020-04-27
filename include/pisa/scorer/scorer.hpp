#pragma once

#include <string>
#include <type_traits>

#include "bm25.hpp"
#include "dph.hpp"
#include "index_scorer.hpp"
#include "pl2.hpp"
#include "qld.hpp"
#include "quantized.hpp"
#include "spdlog/spdlog.h"

struct ScorerParams {
    explicit ScorerParams(std::string scorer_name) : name(std::move(scorer_name)) {}

    std::string name;
    float bm25_b = 0.4;
    float bm25_k1 = 0.9;
    float pl2_c = 1;
    float qld_mu = 1000;
};

namespace pisa { namespace scorer {
    auto from_params =
        [](const ScorerParams& params,
           auto const& wdata) -> std::unique_ptr<index_scorer<std::decay_t<decltype(wdata)>>> {
        if (params.name == "bm25") {
            return std::make_unique<bm25<std::decay_t<decltype(wdata)>>>(
                wdata, params.bm25_b, params.bm25_k1);
        }
        if (params.name == "qld") {
            return std::make_unique<qld<std::decay_t<decltype(wdata)>>>(wdata, params.qld_mu);
        }
        if (params.name == "pl2") {
            return std::make_unique<pl2<std::decay_t<decltype(wdata)>>>(wdata, params.pl2_c);
        }
        if (params.name == "dph") {
            return std::make_unique<dph<std::decay_t<decltype(wdata)>>>(wdata);
        }
        if (params.name == "quantized") {
            return std::make_unique<quantized<std::decay_t<decltype(wdata)>>>(wdata);
        }
        spdlog::error("Unknown scorer {}", params.name);
        std::abort();
    };
}}  // namespace pisa::scorer
