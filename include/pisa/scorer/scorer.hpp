#pragma once

#include <string>

#include "spdlog/spdlog.h"
#include "scorer.hpp"
#include "qld.hpp"
#include "pl2.hpp"
#include "bm25.hpp"
#include "dph.hpp"

namespace pisa {
namespace scorer{
auto from_name = [](std::string const &scorer_name, auto const &wdata) -> std::unique_ptr<index_scorer<decltype(wdata)>> {
    if (scorer_name == "bm25") {
        return std::make_unique<bm25<decltype(wdata)>>(wdata);
    } else if (scorer_name == "qld") {
        return std::make_unique<qld<decltype(wdata)>>(wdata);
    } else if (scorer_name == "pl2") {
        return std::make_unique<pl2<decltype(wdata)>>(wdata);
    } else if (scorer_name == "dph") {
        return std::make_unique<dph<decltype(wdata)>>(wdata);
    } else {
        spdlog::error("Unknown scorer {}", scorer_name);
        std::abort();

    }
};
}
} // namespace pisa
