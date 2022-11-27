#include <iostream>

#include <CLI/CLI.hpp>
#include <range/v3/view/iota.hpp>

#include "app.hpp"
#include "binary_collection.hpp"
#include "io.hpp"
#include "memory_source.hpp"
#include "payload_vector.hpp"
#include "util/util.hpp"

using namespace pisa;

[[nodiscard]] auto
print_function(std::optional<std::string> const& map_file, std::optional<std::string> const& lex_file)
    -> std::function<void(std::uint32_t)>
{
    if (map_file) {
        return [loaded_map = pisa::io::read_string_vector(*map_file)](std::uint32_t term) {
            std::cout << loaded_map.at(term) << ' ';
        };
    }
    if (lex_file) {
        auto source =
            std::make_shared<pisa::MemorySource>(pisa::MemorySource::mapped_file(*lex_file));
        auto lexicon = Payload_Vector<>::from(*source);
        return [source = std::move(source), lexicon](std::uint32_t term) {
            std::cout << lexicon[term] << ' ';
        };
    }
    return [](auto const& term) { std::cout << term << " "; };
}

int main(int argc, char** argv)
{
    std::string collection_file;
    std::optional<std::string> map_file{};
    std::optional<std::string> lex_file{};
    std::size_t first, last;

    pisa::App<pisa::arg::LogLevel> app{"Reads binary collection to stdout."};
    app.add_option("-c,--collection", collection_file, "Collection file path.")->required();
    auto maptext = app.add_option(
        "--maptext",
        map_file,
        "ID to string mapping in text file format. "
        "Line n is the string associated with ID n. "
        "E.g., if used to read a document from a forward index, this would be the `.terms` "
        "file, which maps term IDs to their string reperesentations.");
    auto maplex = app.add_option(
        "--maplex",
        lex_file,
        "ID to string mapping in lexicon binary file format. "
        "E.g., if used to read a document from a forward index, this would be the `.termlex` "
        "file, which maps term IDs to their string reperesentations.");
    maptext->excludes(maplex);
    maplex->excludes(maptext);
    auto* entry_cmd = app.add_subcommand("entry", "Reads single entry.");
    entry_cmd->add_option("n", first, "Entry number.")->required();
    auto* range_cmd = app.add_subcommand("range", "Reads a range of entries.");
    range_cmd->add_option("first", first, "Start reading from this entry.")->required();
    range_cmd->add_option(
        "last",
        last,
        "End reading at this entry. "
        "If not defined, read until the end of the collection.");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    try {
        auto print = print_function(map_file, lex_file);

        binary_collection coll(collection_file.c_str());
        auto iter = coll.begin();
        for ([[maybe_unused]] auto idx: ranges::views::iota(0U, first)) {
            ++iter;
        }

        auto end = [&] {
            if (range_cmd->parsed()) {
                if (range_cmd->count("last") == 0U) {
                    return coll.end();
                }
                if (last < first) {
                    throw std::invalid_argument(
                        "Last entry index must be greater or equal to first.");
                }
                return std::next(iter, last - first + 1);
            }
            return std::next(iter);
        }();

        for (; iter != end; ++iter) {
            auto sequence = *iter;
            for (auto term: sequence) {
                print(term);
            }
            std::cout << '\n';
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
