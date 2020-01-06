#include <iostream>

#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    pisa::App<arg::Index, arg::Query<arg::QueryMode::Unranked>> app(
        "Filters out empty queries against a v1 index.");
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    auto queries = app.queries(meta);
    for (auto&& query : queries) {
        if (not query.get_term_ids().empty()) {
            std::cout << *query.to_json() << '\n';
        }
    }
    return 0;
}
