#include <string>

#include "CLI/CLI.hpp"
#include "tbb/task_scheduler_init.h"
#include "Porter2/Porter2.hpp"

#include "forward_index_builder.hpp"

using ds2i::logger;
using namespace ds2i;

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
    logger() << "Number of threads: " << threads << '\n';

    if (format == "plaintext") {
        Forward_Index_Builder<Plaintext_Record> builder;
        builder.build(std::cin,
                      output_filename,
                      [](std::istream &in) -> std::optional<Plaintext_Record> {
                          Plaintext_Record record;
                          if (in >> record) {
                              return record;
                          }
                          return std::nullopt;
                      },
                      [&](std::string const &term) -> std::string { return term; },
                      batch_size,
                      threads);
    } else if (format == "warc") {
        Forward_Index_Builder<Warc_Record> builder;
        builder.build(std::cin,
                      output_filename,
                      [](std::istream &in) -> std::optional<Warc_Record> {
                          Warc_Record record;
                          if (read_warc_record(in, record)) {
                              return record;
                          }
                          return std::nullopt;
                      },
                      [&](std::string const &term) -> std::string {
                          return stem::Porter2{}.stem(tolower(term));
                      },
                      batch_size,
                      threads);
    }

    return 0;
}
