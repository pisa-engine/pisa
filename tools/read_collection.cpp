#include <iostream>

#include <CLI/CLI.hpp>
#include <range/v3/view/iota.hpp>

#include "binary_collection.hpp"
#include "io.hpp"
#include "memory_source.hpp"
#include "payload_vector.hpp"
#include "util/util.hpp"

using namespace pisa;

int main(int argc, char** argv)
{
    std::string collection_file;
    std::optional<std::string> map_file{};
    std::optional<std::string> lex_file{};
    std::ptrdiff_t first, last;

    CLI::App app{"Reads binary collection to stdout."};
    app.add_option("-c,--collection", collection_file, "Collection file name")->required();

    auto maptext =
        app.add_option("--maptext", map_file, "Text file mapping each line number (ID) to a string");
    auto maplex = app.add_option("--maplex", lex_file, "Lexicon file mapping IDs to strings");
    maptext->excludes(maplex);
    maplex->excludes(maptext);

    app.add_option("first", first, "First element number")->required();
    app.add_option(
        "last",
        last,
        "Open-ended end of range of elements to read. "
        "If not defined, only one element is read.");
    CLI11_PARSE(app, argc, argv);

    std::function<void(std::uint32_t)> print = [](auto const& term) { std::cout << term << " "; };

    try {
        if (map_file) {
            print = [loaded_map = pisa::io::read_string_vector(*map_file)](std::uint32_t term) {
                std::cout << loaded_map.at(term) << ' ';
            };
        }

        if (lex_file) {
            auto source =
                std::make_shared<pisa::MemorySource>(pisa::MemorySource::mapped_file(*lex_file));
            auto lexicon = Payload_Vector<>::from(*source);
            print = [source = std::move(source), lexicon](std::uint32_t term) {
                std::cout << lexicon[term] << ' ';
            };
        }

        binary_collection coll(collection_file.c_str());
        auto iter = coll.begin();
        for ([[maybe_unused]] auto idx: ranges::views::iota(0, first)) {
            ++iter;
        }

        if (app.count("last") == 0U) {
            last = first + 1;
        }

        for (; first < last; ++first) {
            auto sequence = *iter;
            for (auto term: sequence) {
                print(term);
            }
            std::cout << '\n';
            ++iter;
        }
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
        return 1;
    } catch (...) {
        spdlog::error("Unknown error");
        return 1;
    }

    return 0;
}
