#include <CLI/CLI.hpp>
#include <mio/mmap.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "payload_vector.hpp"

using namespace pisa;

int main(int argc, char** argv)
{
    std::string text_file;
    std::string lexicon_file;
    std::size_t idx;
    std::string value;

    pisa::App<pisa::arg::LogLevel> app{"Build, print, or query lexicon"};
    app.require_subcommand();
    auto build = app.add_subcommand("build", "Build a lexicon");
    build->add_option("input", text_file, "Input text file")->required();
    build->add_option("output", lexicon_file, "Output file")->required();
    auto lookup = app.add_subcommand("lookup", "Retrieve the payload at index");
    lookup->add_option("lexicon", lexicon_file, "Lexicon file path")->required();
    lookup->add_option("idx", idx, "Index of requested element")->required();
    auto rlookup = app.add_subcommand("rlookup", "Retrieve the index of payload");
    rlookup->add_option("lexicon", lexicon_file, "Lexicon file path")->required();
    rlookup->add_option("value", value, "Requested value")->required();
    auto print = app.add_subcommand("print", "Print elements line by line");
    print->add_option("lexicon", lexicon_file, "Lexicon file path")->required();
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    try {
        if (*build) {
            std::ifstream is(text_file);
            encode_payload_vector(
                std::istream_iterator<io::Line>(is), std::istream_iterator<io::Line>())
                .to_file(lexicon_file);
            return 0;
        }
        mio::mmap_source m(lexicon_file.c_str());
        auto lexicon = Payload_Vector<>::from(m);
        if (*print) {
            for (auto const& elem: lexicon) {
                std::cout << elem << '\n';
            }
            return 0;
        }
        if (*lookup) {
            if (idx < lexicon.size()) {
                std::cout << lexicon[idx] << '\n';
                return 0;
            }
            spdlog::error("Requested index {} too large for vector of size {}", idx, lexicon.size());
            return 1;
        }
        if (*rlookup) {
            auto pos = pisa::binary_search(lexicon.begin(), lexicon.end(), std::string_view(value));
            if (pos) {
                std::cout << *pos << '\n';
                return 0;
            }
            spdlog::error("Requested term {} was not found", value);
            return 1;
        }
        return 1;
    } catch (std::runtime_error const& err) {
        spdlog::error("{}", err.what());
        return 0;
    }
}
