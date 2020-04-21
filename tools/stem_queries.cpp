#include <fstream>
#include <string>

#include "pisa/query/queries.hpp"
#include "pisa/query/term_processor.hpp"
#include "pisa/tokenizer.hpp"
#include <boost/algorithm/string/join.hpp>

#include "CLI/CLI.hpp"
#include "io.hpp"

int main(int argc, char const* argv[])
{
    std::string input_filename;
    std::string output_filename;
    std::optional<std::string> stemmer;

    CLI::App app{"A tool for stemming PISA queries."};
    app.add_option("-i,--input", input_filename, "Query input file")->required();
    app.add_option("-o,--output", output_filename, "Query output file")->required();
    app.add_option("--stemmer", stemmer, "Stemmer")->required();

    CLI11_PARSE(app, argc, argv);

    std::ofstream output_file;
    output_file.open(output_filename);

    auto input_file = std::ifstream(input_filename);
    pisa::io::for_each_line(input_file, [&](std::string const& query_string) {
        auto [id, raw_query] = pisa::split_query_at_colon(query_string);
        std::vector<std::string> stemmed_terms;
        auto stem = pisa::term_processor(stemmer);
        pisa::TermTokenizer tokenizer(raw_query);
        for (auto term_iter = tokenizer.begin(); term_iter != tokenizer.end(); ++term_iter) {
            stemmed_terms.push_back(std::move(stem(*term_iter)));
        }
        if (id) {
            output_file << *(id) << ":";
        }
        using boost::algorithm::join;
        output_file << join(stemmed_terms, " ") << "\n";
    });
}
