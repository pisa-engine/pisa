#include <string>

#include "CLI/CLI.hpp"
#include "Porter2/Porter2.hpp"
#include "spdlog/spdlog.h"
#include "tbb/task_scheduler_init.h"
#include "warcpp/warcpp.hpp"

#include "forward_index_builder.hpp"

using namespace pisa;

int main(int argc, char **argv) {

    std::string input_basename;
    std::string output_filename;
    std::string format = "plaintext";
    size_t      threads = std::thread::hardware_concurrency();
    ptrdiff_t   batch_size = 100'000;

    CLI::App app{"parse_collection - parse collection and store as forward index."};
    app.add_option("-o,--output", output_filename, "Forward index filename")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    app.add_option(
        "-b,--batch-size", batch_size, "Number of documents to process in one thread", true);
    app.add_option("-f,--format", format, "Input format", true);
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    if (format == "plaintext") {
        Forward_Index_Builder<Plaintext_Record> builder;
        builder.build(
            std::cin,
            output_filename,
            [](std::istream &in) -> std::optional<Plaintext_Record> {
                Plaintext_Record record;
                if (in >> record) {
                    return record;
                }
                return std::nullopt;
            },
            [&](std::string &&term) -> std::string { return std::forward<std::string>(term); },
            parse_plaintext_content,
            batch_size,
            threads);
    } else if (format == "warc") {
        Forward_Index_Builder<warcpp::Warc_Record> builder;
        builder.build(std::cin,
                      output_filename,
                      [](std::istream &in) -> std::optional<warcpp::Warc_Record> {
                          warcpp::Warc_Record record;
                          if (warcpp::read_warc_record(in, record)) {
                              return std::make_optional(record);
                          }
                          return std::nullopt;
                      },
                      [&](std::string &&term) -> std::string {
                          std::transform(term.begin(),
                                         term.end(),
                                         term.begin(),
                                         [](unsigned char c) { return std::tolower(c); });
                          return stem::Porter2{}.stem(term);
                      },
                      parse_html_content,
                      batch_size,
                      threads);
    }

    return 0;
}
