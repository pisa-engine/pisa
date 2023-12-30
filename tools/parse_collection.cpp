#include <filesystem>
#include <string>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>

#include "app.hpp"
#include "forward_index_builder.hpp"
#include "parser.hpp"

using namespace pisa;

int main(int argc, char** argv) {
    auto valid_basename = [](std::string const& basename) {
        std::filesystem::path p(basename);
        auto parent = p.parent_path();
        if (not std::filesystem::exists(parent) or not std::filesystem::is_directory(parent)) {
            return fmt::format(
                "Basename {} invalid: path {} is not an existing directory", basename, parent.string()
            );
        }
        return std::string();
    };

    std::string input_basename;
    std::string output_filename;
    std::string format = "plaintext";
    ptrdiff_t batch_size = 100'000;

    pisa::App<pisa::arg::LogLevel, pisa::arg::Threads, pisa::arg::Analyzer> app{
        "parse_collection - parse collection and store as forward index."
    };
    app.add_option("-o,--output", output_filename, "Forward index filename")
        ->required()
        ->check(valid_basename);
    app.add_option("-b,--batch-size", batch_size, "Number of documents to process in one thread")
        ->capture_default_str();
    app.add_option("-f,--format", format, "Input format")->capture_default_str();

    size_t batch_count, document_count;
    CLI::App* merge_cmd = app.add_subcommand(
        "merge",
        "Merge previously produced batch files. "
        "When parsing process was killed during merging, "
        "use this command to finish merging without "
        "having to restart building batches."
    );
    merge_cmd->add_option("--batch-count", batch_count, "Number of batches")->required();
    merge_cmd->add_option("--document-count", document_count, "Number of documents")->required();

    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, app.threads() + 1);
    spdlog::info("Number of worker threads: {}", app.threads());

    try {
        Forward_Index_Builder builder;
        if (*merge_cmd) {
            builder.merge(output_filename, document_count, batch_count);
        } else {
            builder.build(
                std::cin,
                output_filename,
                record_parser(format, std::cin),
                std::make_shared<TextAnalyzer>(app.text_analyzer()),
                batch_size,
                app.threads() + 1
            );
        }
    } catch (std::exception& err) {
        spdlog::error(err.what());
        return EXIT_FAILURE;
    }
}
