#include <string>

#include <CLI/CLI.hpp>
#include <Porter2/Porter2.hpp>
#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <boost/te.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>
#include <trecpp/trecpp.hpp>
#include <warcpp/warcpp.hpp>

#include "forward_index_builder.hpp"

using namespace pisa;

template <typename ReadSubsequentRecordFn>
[[nodiscard]] auto trec_record_parser(ReadSubsequentRecordFn read_subsequent_record)
{
    return
        [=](std::istream &in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto record = trecpp::match(
                    read_subsequent_record(in),
                    [](trecpp::Record const &rec) {
                        return std::make_optional<Document_Record>(rec);
                    },
                    [](trecpp::Error const &error) {
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
    if (type == "trectext") {
        return trec_record_parser(trecpp::text::read_subsequent_record);
    }
    if (type == "trecweb") {
        return trec_record_parser(trecpp::web::read_subsequent_record);
    }
    if (type == "warc") {
        return [](std::istream &in) -> std::optional<Document_Record> {
            while (not in.eof()) {
                auto record = warcpp::match(
                    warcpp::read_subsequent_record(in),
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
    if (*type == "krovetz") {
        return [](std::string &&term) -> std::string {
            std::transform(term.begin(), term.end(), term.begin(), [](unsigned char c) {
                return std::tolower(c);
            });
            return stem::KrovetzStemmer{}.kstem_stemmer(term);
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

int main(int argc, char **argv) {

    auto valid_basename = [](std::string const &basename) {
        boost::filesystem::path p(basename);
        auto parent = p.parent_path();
        if (not boost::filesystem::exists(parent) or not boost::filesystem::is_directory(parent)) {
            return fmt::format("Basename {} invalid: path {} is not an existing directory",
                               basename,
                               parent.string());
        }
        return std::string();
    };

    std::string input_basename;
    std::string output_filename;
    std::string format = "plaintext";
    size_t      threads = std::thread::hardware_concurrency();
    ptrdiff_t   batch_size = 100'000;
    std::optional<std::string> stemmer = std::nullopt;
    std::optional<std::string> content_parser_type = std::nullopt;
    bool debug = false;

    CLI::App app{"parse_collection - parse collection and store as forward index."};
    app.add_option("-o,--output", output_filename, "Forward index filename")
        ->required()
        ->check(valid_basename);
    app.add_option("-j,--threads", threads, "Thread count");
    app.add_option(
        "-b,--batch-size", batch_size, "Number of documents to process in one thread", true);
    app.add_option("-f,--format", format, "Input format", true);
    app.add_option("--stemmer", stemmer, "Stemmer type");
    app.add_option("--content-parser", content_parser_type, "Content parser type");
    app.add_flag("--debug", debug, "Print debug messages");

    size_t batch_count, document_count;
    CLI::App *merge_cmd = app.add_subcommand("merge",
                                             "Merge previously produced batch files. "
                                             "When parsing process was killed during merging, "
                                             "use this command to finish merging without "
                                             "having to restart building batches.");
    merge_cmd->add_option("--batch-count", batch_count, "Number of batches")->required();
    merge_cmd->add_option("--document-count", document_count, "Number of documents")->required();

    CLI11_PARSE(app, argc, argv);

    if (debug) {
        spdlog::set_level(spdlog::level::debug);
    }

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    Forward_Index_Builder builder;
    if (*merge_cmd) {
        builder.merge(output_filename, document_count, batch_count);
    } else {
        builder.build(std::cin,
                      output_filename,
                      record_parser(format),
                      term_processor(stemmer),
                      content_parser(content_parser_type),
                      batch_size,
                      threads);
    }

    return 0;
}
