// Copyright 2024 PISA developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdlib>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <mio/mmap.hpp>

#include "app.hpp"
#include "pisa/io.hpp"
#include "pisa/lookup_table.hpp"

struct Arguments {
    std::string lexicon_file{};
    std::string value{};
    std::optional<std::size_t> at{};
    std::optional<std::size_t> from{};
    std::optional<std::size_t> last{};
    std::optional<std::size_t> count{};
};

struct Commands {
    CLI::App* build{};
    CLI::App* find{};
    CLI::App* print{};
};

auto build_cmd(CLI::App& app, Arguments& args) {
    auto cmd = app.add_subcommand("build", "Builds a lookup table from stdin");
    cmd->add_option("-o,--output", args.lexicon_file, "Binary output file")->required();
    return cmd;
}

auto find_cmd(CLI::App& app, Arguments& args) {
    auto cmd = app.add_subcommand("find", "Finds the given value and returns its position");
    cmd->add_option("table", args.lexicon_file, "Path to lookup table")->required();
    cmd->add_option("value", args.value, "Value to find")->required();
    return cmd;
}

auto print_cmd(CLI::App& app, Arguments& args) {
    auto cmd = app.add_subcommand("print", "Prints values");
    cmd->add_option("table", args.lexicon_file, "Path to lookup table")->required();
    auto at = cmd->add_option("--at", args.at, "Position of a single element to print");
    cmd->add_option("--from", args.from, "Starting position")->excludes(at);
    auto to = cmd->add_option("--to", args.last, "Last position")->excludes(at);
    cmd->add_option("--count", args.count, "Number of values to print")->excludes(at)->excludes(to);
    return cmd;
}

void build(Arguments const& args) {
    std::vector<std::string> values;
    std::size_t payload_size = 0;
    bool sorted = true;
    pisa::io::for_each_line(std::cin, [&values, &payload_size, &sorted](std::string&& value) {
        payload_size += value.size();
        values.push_back(std::move(value));
        if (sorted && payload_size > 0 && value <= values.back()) {
            sorted = false;
        }
    });
    std::uint8_t flags = 0;
    if (sorted) {
        flags |= ::pisa::lt::v1::flags::SORTED;
    }
    if (payload_size >= (1UL << 32) - 1) {
        flags |= ::pisa::lt::v1::flags::WIDE_OFFSETS;
    }
    auto encoder = ::pisa::LookupTableEncoder::v1(::pisa::lt::v1::Flags(flags));
    for (auto& value: values) {
        encoder.insert(value);
    }
    std::ofstream out(args.lexicon_file);
    encoder.encode(out);
}

void get(pisa::LookupTable const& table, std::size_t idx) {
    if (idx >= table.size()) {
        throw std::runtime_error(
            fmt::format("position {} in a table of size {} is out of bounds", idx, table.size())
        );
    }
    auto value = table.at<std::string>(idx);
    std::cout << value;
}

void find(pisa::LookupTable const& table, std::string const& value) {
    auto idx = table.find(value);
    if (idx.has_value()) {
        std::cout << *idx;
    } else {
        throw std::runtime_error(fmt::format("value '{}' not found", value));
    }
}

void print(pisa::LookupTable const& table, Arguments const& args) {
    if (args.at.has_value()) {
        get(table, *args.at);
        return;
    }

    std::size_t first = 0;
    std::size_t last = table.size() - 1;

    if (args.from.has_value()) {
        first = *args.from;
    }
    if (args.last.has_value()) {
        last = *args.last;
    }
    if (args.count.has_value()) {
        last = *args.count + first - 1;
    }

    if (first >= table.size()) {
        throw std::runtime_error(
            fmt::format(
                "starting position {} in a table of size {} is out of bounds", first, table.size()
            )
        );
    }
    if (last >= table.size()) {
        throw std::runtime_error(
            fmt::format("last position {} in a table of size {} is out of bounds", last, table.size())
        );
    }

    for (auto pos = first; pos <= last; ++pos) {
        std::cout << table.at<std::string_view>(pos) << '\n';
    }
}

int main(int argc, char** argv) {
    Arguments args;
    Commands cmds;

    pisa::App<pisa::arg::LogLevel> app{"Builds, prints, or queries lookup table"};
    app.require_subcommand();
    cmds.build = build_cmd(app, args);
    cmds.find = find_cmd(app, args);
    cmds.print = print_cmd(app, args);
    CLI11_PARSE(app, argc, argv);

    try {
        if (*cmds.build) {
            build(args);
        } else {
            mio::mmap_source mem(args.lexicon_file.c_str());
            auto table = pisa::LookupTable::from_bytes(
                std::span(reinterpret_cast<std::byte const*>(mem.data()), mem.size())
            );
            if (*cmds.find) {
                find(table, args.value);
            } else if (*cmds.print) {
                print(table, args);
            }
        }
    } catch (std::exception const& err) {
        std::cerr << "error: " << err.what() << '\n';
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
