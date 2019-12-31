#include <string_view>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>

#include "sequence/partitioned_sequence.hpp"
#include "v1/bit_sequence_cursor.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/index_builder.hpp"
#include "v1/index_metadata.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/sequence/partitioned_sequence.hpp"
#include "v1/sequence/positive_sequence.hpp"
#include "v1/types.hpp"

using std::literals::string_view_literals::operator""sv;

using pisa::v1::compress_binary_collection;
using pisa::v1::DocumentBitSequenceWriter;
using pisa::v1::DocumentBlockedWriter;
using pisa::v1::EncodingId;
using pisa::v1::make_index_builder;
using pisa::v1::PartitionedSequence;
using pisa::v1::PayloadBitSequenceWriter;
using pisa::v1::PayloadBlockedWriter;
using pisa::v1::PositiveSequence;
using pisa::v1::RawWriter;
using pisa::v1::verify_compressed_index;

auto document_encoding(std::string_view name) -> std::uint32_t
{
    if (name == "raw"sv) {
        return EncodingId::Raw;
    }
    if (name == "simdbp"sv) {
        return EncodingId::BlockDelta | EncodingId::SimdBP;
    }
    if (name == "pef"sv) {
        return EncodingId::BitSequence | EncodingId::PEF;
    }
    spdlog::error("Unknown encoding: {}", name);
    std::exit(1);
}

auto frequency_encoding(std::string_view name) -> std::uint32_t
{
    if (name == "raw"sv) {
        return EncodingId::Raw;
    }
    if (name == "simdbp"sv) {
        return EncodingId::Block | EncodingId::SimdBP;
    }
    if (name == "pef"sv) {
        return EncodingId::BitSequence | EncodingId::PositiveSeq;
    }
    spdlog::error("Unknown encoding: {}", name);
    std::exit(1);
}

int main(int argc, char** argv)
{
    std::string input;
    std::string fwd;
    std::string output;
    std::string encoding;
    std::size_t threads = std::thread::hardware_concurrency();

    CLI::App app{"Compresses a given binary collection to a v1 index."};
    app.add_option("-i,--inv", input, "Input collection basename")->required();
    // TODO(michal): Potentially, this would be removed once inv contains necessary info.
    app.add_option("-f,--fwd", fwd, "Input forward index")->required();
    app.add_option("-o,--output", output, "Output basename")->required();
    app.add_option("-j,--threads", threads, "Number of threads");
    app.add_option("-e,--encoding", encoding, "Number of threads")->required();
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    auto build =
        make_index_builder(std::make_tuple(RawWriter<std::uint32_t>{},
                                           DocumentBlockedWriter<::pisa::simdbp_block>{},
                                           DocumentBitSequenceWriter<PartitionedSequence<>>{}),
                           std::make_tuple(RawWriter<std::uint32_t>{},
                                           PayloadBlockedWriter<::pisa::simdbp_block>{},
                                           PayloadBitSequenceWriter<PositiveSequence<>>{}));
    build(document_encoding(encoding),
          frequency_encoding(encoding),
          [&](auto document_writer, auto payload_writer) {
              compress_binary_collection(input,
                                         fwd,
                                         output,
                                         threads,
                                         make_writer(std::move(document_writer)),
                                         make_writer(std::move(payload_writer)));
          });
    auto errors = verify_compressed_index(input, output);
    if (not errors.empty()) {
        if (errors.size() > 10) {
            std::cerr << "Detected more than 10 errors, printing head:\n";
            errors.resize(10);
        }
        for (auto const& error : errors) {
            std::cerr << error << '\n';
        }
        return 1;
    }

    std::cout << "Success.";
    return 0;
}
