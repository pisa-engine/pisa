#pragma once

#include <optional>
#include <string>

#include <CLI/CLI.hpp>

namespace pisa {

struct QueryApp : public CLI::App {
    explicit QueryApp(std::string description) : CLI::App(std::move(description))
    {
        add_option("-i,--index",
                   yml,
                   "Path of .yml file of an index "
                   "(if not provided, it will be looked for in the current directory)",
                   false);
        add_option("-q,--query", query_file, "Path to file with queries", false);
        add_option("-k", k, "The number of top results to return", true);
        add_option("--terms", terms_file, "Overrides document lexicon from .yml (if defined).");
        add_option("--documents",
                   documents_file,
                   "Overrides document lexicon from .yml (if defined). Required otherwise.");
        add_flag("--benchmark", is_benchmark, "Run benchmark");
        add_flag("--precomputed", precomputed, "Use precomputed scores");
    }

    std::optional<std::string> yml{};
    std::optional<std::string> query_file{};
    std::optional<std::string> terms_file{};
    std::optional<std::string> documents_file{};
    int k = 1'000;
    bool is_benchmark = false;
    bool precomputed = false;
};

} // namespace pisa
