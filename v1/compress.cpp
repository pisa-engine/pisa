#include <CLI/CLI.hpp>

#include "binary_freq_collection.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/types.hpp"

using pisa::v1::compress_binary_collection;
using pisa::v1::EncodingId;
using pisa::v1::IndexBuilder;
using pisa::v1::RawWriter;
using pisa::v1::verify_compressed_index;

int main(int argc, char **argv)
{
    std::string input;
    std::string output;
    std::size_t threads = std::thread::hardware_concurrency();

    CLI::App app{"Compresses a given binary collection to a v1 index."};
    app.add_option("-c,--collection", input, "Input collection basename")->required();
    app.add_option("-o,--output", output, "Output basename")->required();
    app.add_option("-j,--threads", threads, "Number of threads");
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    IndexBuilder<RawWriter<std::uint32_t>> build(RawWriter<std::uint32_t>{});
    build(EncodingId::Raw, [&](auto writer) {
        auto frequency_writer = writer;
        compress_binary_collection(input,
                                   output,
                                   threads,
                                   make_writer(std::move(writer)),
                                   make_writer(std::move(frequency_writer)));
    });
    auto errors = verify_compressed_index(input, output);
    if (not errors.empty()) {
        if (errors.size() > 10) {
            std::cerr << "Detected more than 10 errors, printing head:\n";
            errors.resize(10);
        }
        for (auto const &error : errors) {
            std::cerr << error << '\n';
        }
        return 1;
    }

    std::cout << "Success.";
    return 0;
}
