#pragma once

#include <string>

#include "bm25.hpp"
#include "dph.hpp"
#include "index_scorer.hpp"
#include "pl2.hpp"
#include "qld.hpp"
#include "spdlog/spdlog.h"

namespace pisa {

template <typename WandData, typename Func>
void with_scorer(std::string const &scorer_name, WandData const &wdata, Func func)
{
    if (scorer_name == "bm25") {
        func(bm25<WandData>(wdata));
    } else if (scorer_name == "qld") {
        func(qld<WandData>(wdata));
    } else if (scorer_name == "pl2") {
        func(pl2<WandData>(wdata));
    } else if (scorer_name == "dph") {
        func(dph<WandData>(wdata));
    } else {
        spdlog::error("Unknown scorer {}", scorer_name);
        std::abort();
    }
}

namespace scorer {
    auto from_name = [](std::string const &scorer_name,
                        auto const &wdata) -> std::unique_ptr<index_scorer<decltype(wdata)>> {
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
