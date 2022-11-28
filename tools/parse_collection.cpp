#include <string>

#include <CLI/CLI.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>

#include "app.hpp"
#include "forward_index_builder.hpp"
#include "parser.hpp"
#include "query/term_processor.hpp"

using namespace pisa;

int main(int argc, char** argv)
{
    auto valid_basename = [](std::string const& basename) {
        boost::filesystem::path p(basename);
        auto parent = p.parent_path();
        if (not boost::filesystem::exists(parent) or not boost::filesystem::is_directory(parent)) {
            return fmt::format(
                "Basename {} invalid: path {} is not an existing directory",
                basename,
                parent.string());
        }
        return std::string();
    };

    std::string input_basename;
    std::string output_filename;
    std::string format = "plaintext";
    size_t threads = std::thread::hardware_concurrency();
    ptrdiff_t batch_size = 100'000;
    std::optional<std::string> stemmer = std::nullopt;
    std::optional<std::string> content_parser_type = std::nullopt;

    pisa::App<pisa::arg::LogLevel> app{
        "parse_collection - parse collection and store as forward index."};
    app.add_option("-o,--output", output_filename, "Forward index filename")
        ->required()
        ->check(valid_basename);
    app.add_option("-j,--threads", threads, "Thread count");
    app.add_option(
        "-b,--batch-size", batch_size, "Number of documents to process in one thread", true);
    app.add_option("-f,--format", format, "Input format", true);
    app.add_option("--stemmer", stemmer, "Stemmer type");
    app.add_option("--content-parser", content_parser_type, "Content parser type");

    size_t batch_count, document_count;
    CLI::App* merge_cmd = app.add_subcommand(
        "merge",
        "Merge previously produced batch files. "
        "When parsing process was killed during merging, "
        "use this command to finish merging without "
        "having to restart building batches.");
    merge_cmd->add_option("--batch-count", batch_count, "Number of batches")->required();
    merge_cmd->add_option("--document-count", document_count, "Number of documents")->required();

    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, threads + 1);
    spdlog::info("Number of worker threads: {}", threads);

    try {
        Forward_Index_Builder builder;
        if (*merge_cmd) {
            builder.merge(output_filename, document_count, batch_count);
        } else {
            builder.build(
                std::cin,
                output_filename,
                record_parser(format, std::cin),
                term_transformer_builder(stemmer),
                content_parser(content_parser_type),
                batch_size,
                threads);
        }
    } catch (std::exception& err) {
        spdlog::error(err.what());
        return EXIT_FAILURE;
    }
}
