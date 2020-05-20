#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "binary_index.hpp"
#include "query.hpp"

using pisa::TermPair;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string output;

    CLI::App app{"Build pair index."};
    pisa::PairIndexArgs args(&app);
    app.add_option("-o,--output", output, "Output basename");
    CLI11_PARSE(app, argc, argv);

    std::vector<TermPair> pairs;
    args.resolved_query_reader().for_each([&pairs](auto query) {
        auto request = query.query(pisa::query::unlimited);
        auto term_ids = request.term_ids();
        for (int left = 0; left < term_ids.size(); left += 1) {
            for (int right = left + 1; right < term_ids.size(); right += 1) {
                pairs.emplace_back(term_ids[left], term_ids[right]);
            }
        }
    });

    pisa::build_binary_index(args.index_filename(), pairs, output);
}
