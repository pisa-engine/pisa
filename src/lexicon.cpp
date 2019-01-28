#include <iostream>
#include <sstream>
#include <string>

#include "lexicon.hpp"
#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "CLI/CLI.hpp"

using namespace pisa;

void build_lexicon(std::string const &input_file, std::string const &output_file)
{
    std::ifstream is(input_file);
    std::ofstream os(output_file);
    Lexicon_Data(Line_Iterator(is), Line_Iterator()).serialize(os);
}

int main(int argc, const char **argv)
{
    std::string input;
    std::string lexicon_filename;
    std::string value;
    std::size_t key;

    CLI::App app{"Operations on lexicon"};
    app.require_subcommand(1);

    auto command_with_lexicon = [&](std::string const &name,
                                    std::string const &description) -> CLI::App * {
        auto command = app.add_subcommand(name, description);
        command->add_option("lexicon", lexicon_filename, "Lexicon file", false)
            ->required()
            ->check(CLI::ExistingFile);
        return command;
    };

    CLI::App *build = app.add_subcommand("build", "Build a lexicon");
    build->add_option("-i,--input", input, "input file", false)
        ->check(CLI::ExistingFile)
        ->required();
    build->add_option("-o,--output", lexicon_filename, "output file", false)->required();

    auto size = command_with_lexicon("size", "Number of elements in lexicon");
    auto print = command_with_lexicon("print", "Print all values");
    auto at = command_with_lexicon("at", "Value at position");
    at->add_option("key", key, "Position")->required();
    auto reverse = command_with_lexicon("reverse", "Reverse lookup");
    reverse->add_option("value", value, "Value")->required();

    CLI11_PARSE(app, argc, argv);

    if (*build) {
        build_lexicon(input, lexicon_filename);
        return 0;
    }
    else {
        mio::mmap_source source(lexicon_filename);
        auto lexicon = Lexicon_View::parse(source.data());
        if (*at) {
            std::cout << lexicon[key] << '\n';
        }
        else if (*reverse) {
            auto pos = std::lower_bound(lexicon.begin(), lexicon.end(), value);
            if (pos != lexicon.end() and *pos == value) {
                std::cout << std::distance(lexicon.begin(), pos) << '\n';
            }
        }
        else if (*size) {
            std::cout << lexicon.size() << '\n';
        }
        else if (*print) {
            for (auto val : lexicon) {
                std::cout << val << '\n';
            }
        }
        return 0;
    }
}
