#include <string>

#include <CLI/CLI.hpp>
#include <Porter2/Porter2.hpp>
#include <boost/te.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>
#include <warcpp/warcpp.hpp>

#include "cli.hpp"
#include "forward_index_builder.hpp"

using namespace pisa;

std::function<std::optional<Document_Record>(std::istream &)> record_parser(
    std::string const &type)
{
    if (type == "plaintext") {
        return [](std::istream &in) -> std::optional<Document_Record> {
            Plaintext_Record record;
            if (in >> record) {
                return std::make_optional<Document_Record>(record);
            }
            return std::nullopt;
        };
    }
    if (type == "warc") {
        return [](std::istream &in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto record = warcpp::match(warcpp::read_subsequent_record(in),
                                            [](warcpp::Record const &rec) {
                                                if (not rec.valid_response()) {
                                                    return std::optional<Document_Record>{};
                                                }
                                                return std::make_optional<Document_Record>(rec);
                                            },
                                            [](warcpp::Error const &error) {
                                                spdlog::warn("Skipped invalid record: {}", error);
                                                return std::optional<Document_Record>{};
                                            });
                if (record) {
                    return record;
                }
            }
            return std::nullopt;
        };
    }
    spdlog::error("Unknown record type: {}", type);
    std::abort();
}

std::function<std::string(std::string &&)> term_processor(std::optional<std::string> const &type)
{
    if (not type) {
        return [](std::string &&term) -> std::string { return std::forward<std::string>(term); };
    }
    if (*type == "porter2") {
        return [](std::string &&term) -> std::string {
            std::transform(term.begin(), term.end(), term.begin(), [](unsigned char c) {
                return std::tolower(c);
            });
            return stem::Porter2{}.stem(term);
        };
    }
    spdlog::error("Unknown stemmer type: {}", *type);
    std::abort();
}

std::function<void(std::string &&constent, std::function<void(std::string &&)>)>
content_parser(std::optional<std::string> const &type) {
    if (not type) {
        return parse_plaintext_content;
    }
    if (*type == "html") {
        return parse_html_content;
    }
    spdlog::error("Unknown content parser type: {}", *type);
    std::abort();
}

int main(int argc, char **argv)
{
    CLI::App app{"parse_collection - parse collection and store as forward index."};
    auto output = option<std::string>(app, "-o,--ouput", "Forward index filename")
                      .with(required, check(valid_basename));
    auto threads = options::threads(app);
    auto stemmer = options::stemmer(app);
    auto record_format = options::record_format(app);
    auto content_parser_type = options::content_parser(app);
    auto debug = options::debug(app);
    auto batch_size = options::batch_size(app, default_value = 10'000u);

    CLI::App *merge_cmd = app.add_subcommand("merge",
                                             "Merge previously produced batch files. "
                                             "When parsing process was killed during merging, "
                                             "use this command to finish merging without "
                                             "having to restart building batches.");
    auto batch_count =
        option<std::size_t>(*merge_cmd, "--batch-count", "Number of batches").with(required);
    auto document_count =
        option<std::size_t>(*merge_cmd, "--document-count", "Number of documents").with(required);

    CLI11_PARSE(app, argc, argv);

    if (debug) {
        spdlog::set_level(spdlog::level::debug);
    }

    tbb::task_scheduler_init init(*threads);
    spdlog::info("Number of threads: {}", *threads);

    Forward_Index_Builder builder;
    if (*merge_cmd) {
        builder.merge(*output, *document_count, *batch_count);
    } else {
        builder.build(std::cin,
                      *output,
                      record_parser(*record_format),
                      term_processor(*stemmer),
                      content_parser(*content_parser_type),
                      *batch_size,
                      *threads);
    }

    return 0;
}
