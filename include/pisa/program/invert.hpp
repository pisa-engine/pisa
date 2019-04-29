#pragma once

#include <thread>
#include <variant>

#include <CLI/CLI.hpp>

#include "cli.hpp"

namespace pisa {

struct InvertSettings {
    std::string input_basename{};
    std::string output_basename{};
    std::size_t threads = std::thread::hardware_concurrency();
    std::size_t term_count{};
    std::size_t batch_size = 100'000;

    [[nodiscard]] static auto parse(int argc, char const **argv)
        -> std::variant<InvertSettings, int>
    {
        InvertSettings settings;
        CLI::App app{"Convert forward index into inverted index"};
        app.add_option("-i,--input", settings.input_basename, "Forward index filename")->required();
        app.add_option("-o,--output", settings.output_basename, "Output inverted index basename")
            ->required();
        app.add_option("--term-count", settings.term_count, "Term count")->required();
        cli::options::threads(app, settings);
        cli::options::batch_size(app, settings);

        try {
            app.parse(argc, argv);
            return settings;
        } catch (const CLI::ParseError &e) {
            return app.exit(e);
        }
    }
};

} // namespace pisa::program
