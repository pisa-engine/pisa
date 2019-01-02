#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>

#include "CLI/CLI.hpp"
#include "pstl/algorithm"
#include "pstl/execution"

#include "binary_collection.hpp"
#include "parsing/html.hpp"
#include "parsing/stemmer.hpp"
#include "parsing/warc.hpp"
#include "util/util.hpp"

using ds2i::logger;
using namespace ds2i;

int main(int argc, char **argv) {

    std::string collection_file;
    std::string map_file{};
    std::ptrdiff_t idx;

    CLI::App app{"read_collection - read collections."};
    app.add_option("-c,--collection", collection_file, "Collection file name")->required();
    app.add_option("-m,--map", map_file, "String map file name");
    app.add_option("idx", idx, "Element number")->required();
    CLI11_PARSE(app, argc, argv);

    std::vector<std::string> map;
    if (not map_file.empty()) {
        std::ifstream is(map_file);
        std::string str;
        while (std::getline(is, str)) {
            map.push_back(str);
        }
    }

    binary_collection coll(collection_file.c_str());
    auto iter = ++coll.begin();
    for (; idx > 0; --idx) {
        ++iter;
    }
    auto sequence = *iter;
    std::function<void(std::uint32_t const &)> print = [](auto const &term) {
        std::cout << term << " ";
    };
    if (not map.empty()) {
        print = [&](std::uint32_t const &term) { std::cout << map[term] << " "; };
    }
    for (auto const& term : sequence) {
        print(term);
    }
    std::cout << '\n';

    return 0;
}
