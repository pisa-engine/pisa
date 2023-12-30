#include <fstream>
#include <memory>
#include <string>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "pisa/text_analyzer.hpp"
#include "pisa/token_filter.hpp"
#include "query/query_parser.hpp"
#include "tokenizer.hpp"

int main(int argc, char const* argv[]) {
    std::string input_filename;
    std::string output_filename;
    std::string stemmer;

    pisa::App<pisa::arg::LogLevel> app{"A tool for stemming PISA queries."};
    app.add_option("-i,--input", input_filename, "Query input file")->required();
    app.add_option("-o,--output", output_filename, "Query output file")->required();
    app.add_option("--stemmer", stemmer, "Stemmer")->required();

    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    std::ofstream output_file;
    output_file.open(output_filename);

    auto input_file = std::ifstream(input_filename);

    try {
        auto analyzer = pisa::TextAnalyzer(std::make_unique<pisa::EnglishTokenizer>());
        analyzer.add_token_filter(pisa::stemmer_from_name(stemmer));
        pisa::QueryParser parser(std::move(analyzer));
        pisa::io::for_each_line(input_file, [&](std::string const& line) {
            auto query = parser.parse(line);
            if (query.id()) {
                output_file << *query.id() << ":";
            }
            auto const& terms = query.terms();
            if (!terms.empty()) {
                output_file << terms.front().id;
                for (auto pos = std::next(terms.begin()); pos != terms.end(); std::advance(pos, 1)) {
                    output_file << ' ' << pos->id;
                }
            }
        });
    } catch (const std::invalid_argument& ex) {
        spdlog::error(ex.what());
        std::exit(1);
    }
}
