#include "v1/index_builder.hpp"

namespace pisa::v1 {

auto verify_compressed_index(std::string const &input, std::string_view output)
    -> std::vector<std::string>
{
    std::vector<std::string> errors;
    pisa::binary_freq_collection const collection(input.c_str());
    auto meta = IndexMetadata::from_file(fmt::format("{}.ini", output));
    auto run = index_runner(meta, RawReader<std::uint32_t>{});
    run([&](auto &&index) {
        auto sequence_iter = collection.begin();
        for (auto term = 0; term < index.num_terms(); term += 1, ++sequence_iter) {
            auto document_sequence = sequence_iter->docs;
            auto frequency_sequence = sequence_iter->freqs;
            auto cursor = index.cursor(term);
            if (cursor.size() != document_sequence.size()) {
                errors.push_back(
                    fmt::format("Posting list length mismatch for term {}: expected {} but is {}",
                                term,
                                document_sequence.size(),
                                cursor.size()));
                continue;
            }
            auto dit = document_sequence.begin();
            auto fit = frequency_sequence.begin();
            auto pos = 0;
            while (not cursor.empty()) {
                if (cursor.value() != *dit) {
                    errors.push_back(
                        fmt::format("Document mismatch for term {} at position {}", term, pos));
                }
                if (cursor.payload() != *fit) {
                    errors.push_back(
                        fmt::format("Frequency mismatch for term {} at position {}", term, pos));
                }
                cursor.advance();
                ++dit;
                ++fit;
                ++pos;
            }
        }
    });
    return errors;
}

} // namespace pisa::v1
