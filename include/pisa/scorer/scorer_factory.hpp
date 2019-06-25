#pragma once

#include "scorer.hpp"
#include "ql.hpp"
#include "bm25.hpp"

namespace pisa {

auto get_scorer = [](std::string const &scorer_name, auto const &wdata) -> std::unique_ptr<scorer<decltype(wdata)>> {
    if (scorer_name == "bm25") {
        return std::make_unique<bm25<decltype(wdata)>>(wdata);
    } else if (scorer_name == "qld") {
        return std::make_unique<ql<decltype(wdata)>>(wdata);
    } else {
        spdlog::error("Unknown scorer {}", scorer_name);
        std::abort();

    }
};

} // namespace pisa
