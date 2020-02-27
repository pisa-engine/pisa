#pragma once

#include <string>

#include "bm25.hpp"
#include "dph.hpp"
#include "index_scorer.hpp"
#include "pl2.hpp"
#include "qld.hpp"
#include "quantized.hpp"
#include "spdlog/spdlog.h"

namespace pisa {
namespace scorer {
    auto from_name =
        [](std::string const &scorer_name,
           auto const &wdata) -> std::unique_ptr<index_scorer<decay_t<decltype(wdata)>>> {
        if (scorer_name == "bm25") {
            return std::make_unique<bm25<decay_t<decltype(wdata)>>>(wdata);
        } else if (scorer_name == "qld") {
            return std::make_unique<qld<decay_t<decltype(wdata)>>>(wdata);
        } else if (scorer_name == "pl2") {
            return std::make_unique<pl2<decay_t<decltype(wdata)>>>(wdata);
        } else if (scorer_name == "dph") {
            return std::make_unique<dph<decay_t<decltype(wdata)>>>(wdata);
        } else if (scorer_name == "quantized") {
            return std::make_unique<quantized<decay_t<decltype(wdata)>>>(wdata);
        } else {
            spdlog::error("Unknown scorer {}", scorer_name);
            std::abort();
        }
    };
}
} // namespace pisa
