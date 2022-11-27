#include <fstream>
#include <string>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "pisa/query/query_stemmer.hpp"

int main(int argc, char const* argv[])
{
    std::string input_filename;
    std::string output_filename;
    std::optional<std::string> stemmer;

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
        pisa::QueryStemmer query_stemmer(stemmer);
        pisa::io::for_each_line(input_file, [&](std::string const& line) {
            output_file << query_stemmer(line) << "\n";
        });
    } catch (const std::invalid_argument& ex) {
        spdlog::error(ex.what());
        std::exit(1);
    }
}
