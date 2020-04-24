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
    std::string m_name = "default"; 
    float m_bm25_b = 0.4;
    float m_bm25_k1 = 0.9;
    float m_pl2_c = 1;
    float m_qld_mu = 1000;
};

namespace pisa { namespace scorer {
    auto from_params =
        [](const ScorerParams& params,
           auto const& wdata) -> std::unique_ptr<index_scorer<std::decay_t<decltype(wdata)>>> {
        if (params.m_name == "bm25") {
            return std::make_unique<bm25<std::decay_t<decltype(wdata)>>>(wdata, params.m_bm25_b, params.m_bm25_k1);
        }
        if (params.m_name == "qld") {
            return std::make_unique<qld<std::decay_t<decltype(wdata)>>>(wdata, params.m_qld_mu);
        }
        if (params.m_name == "pl2") {
            return std::make_unique<pl2<std::decay_t<decltype(wdata)>>>(wdata, params.m_pl2_c);
        }
        if (params.m_name == "dph") {
            return std::make_unique<dph<std::decay_t<decltype(wdata)>>>(wdata);
        }
        if (params.m_name == "quantized") {
            return std::make_unique<quantized<std::decay_t<decltype(wdata)>>>(wdata);
        }
        spdlog::error("Unknown scorer {}", params.m_name);
        std::abort();
    };
}}  // namespace pisa::scorer
