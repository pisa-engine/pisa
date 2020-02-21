#include <iostream>

#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "v1/default_index_runner.hpp"
#include "v1/progress_status.hpp"

namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    bool pair_index = false;
    bool term_by_term = false;

    pisa::App<arg::Index> app("Simply counts all postings in the index");
    auto* pairs_opt =
        app.add_flag("--pairs", pair_index, "Count postings in the pair index instead");
    app.add_flag("-t,--terms", term_by_term, "Print posting counts for each term in the index")
        ->excludes(pairs_opt);
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    std::size_t count{0};
    pisa::v1::index_runner(meta)([&](auto&& index) {
        if (pair_index) {
            pisa::v1::ProgressStatus status(
                index.pairs()->size(),
                pisa::v1::DefaultProgressCallback("Counting pair postings"),
                std::chrono::milliseconds(500));
            for (auto term_pair : index.pairs().value()) {
                count +=
                    index.bigram_cursor(std::get<0>(term_pair), std::get<1>(term_pair))->size();
                status += 1;
            }
        } else if (term_by_term) {
            for (pisa::v1::TermId id = 0; id < index.num_terms(); id += 1) {
                std::cout << index.term_posting_count(id) << '\n';
            }
        } else {
            pisa::v1::ProgressStatus status(
                index.num_terms(),
                pisa::v1::DefaultProgressCallback("Counting term postings"),
                std::chrono::milliseconds(500));
            for (pisa::v1::TermId id = 0; id < index.num_terms(); id += 1) {
                count += index.term_posting_count(id);
                status += 1;
            }
        }
    });
    std::cout << count << '\n';
    return 0;
}
