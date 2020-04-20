#pragma once

#include <algorithm>

#include "app.hpp"
#include "binary_freq_collection.hpp"

namespace pisa {

auto reorder_random(std::uint64_t seed) -> bool
{
    return false;
}

auto reorder_url() -> bool
{
    return false;
}

auto reorder_from_input() -> bool
{
    return false;
}

auto reorder_bp() -> bool
{
    return false;
}

auto reorder_docids(ReorderDocuments args) -> bool
{
    if (args.random()) {
        return reorder_random(args.seed());
    }
    if (args.url()) {
        return reorder_url();
    }
    if (auto input = args.order_file(); input) {
        return reorder_from_input();
    }
    if (args.bp()) {
        return reorder_bp();
    }
    spdlog::error("Should be unreachable due to argument constraints!");
    std::exit(1);
}

}  // namespace pisa
