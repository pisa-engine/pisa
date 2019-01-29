#include <iostream>

#include <CLI/CLI.hpp>
#include <range/v3/view/iota.hpp>

#include "binary_collection.hpp"
#include "util/util.hpp"

using namespace pisa;

int main(int argc, char **argv) {

    std::string collection_file;
    std::string map_file{};
    std::ptrdiff_t first, last;

    CLI::App app{"read_collection - read collections."};
    app.add_option("-c,--collection", collection_file, "Collection file name")->required();
    app.add_option("-m,--map", map_file, "String map file name");
    app.add_option("first", first, "Element number")->required();
    app.add_option("last", last, "Element number");
    CLI11_PARSE(app, argc, argv);

    std::function<void(std::uint32_t const &)> print = [](auto const &term) {
        std::cout << term << " ";
    };

    std::vector<std::string> map;
    if (not map_file.empty()) {
        std::ifstream is(map_file);
        std::string str;
        while (std::getline(is, str)) {
            map.push_back(str);
        }
        print = [&](std::uint32_t const &term) { std::cout << map[term] << " "; };
    }

    binary_collection coll(collection_file.c_str());
    auto iter = coll.begin();
    for ([[maybe_unused]] auto idx : ranges::view::iota(0, first)) {
        ++iter;
    }

    if (app.count("last") == 0u) {
        last = first + 1;
    }

    for (; first < last; ++first) {
        auto sequence = *iter;
        for (auto const &term : sequence) {
            print(term);
        }
        std::cout << '\n';
        ++iter;
    }

    return 0;
}
