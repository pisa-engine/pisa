#pragma once

#include <fstream>
#include <iterator>
#include <vector>

#include <fmt/format.h>

#include "binary_freq_collection.hpp"
#include "payload_vector.hpp"

namespace pisa {

auto create_intersection_collection(std::string const &input_basename,
                                    std::string const &output_filename)
{
    std::ofstream doc_stream(fmt::format("{}.bidocs", output_filename));
    std::ofstream freq_stream(fmt::format("{}.bifreqs", output_filename));

    binary_freq_collection input(input_basename.c_str());
    std::uint32_t one = 1;
    doc_stream.write(reinterpret_cast<char const *>(&one), 4);
    std::uint32_t num_documents = input.num_docs();
    doc_stream.write(reinterpret_cast<char const *>(&num_documents), 4);

    std::vector<std::pair<std::uint32_t, std::uint32_t>> mapping;
    mapping.reserve(input.size() * (input.size() - 1) / 2);
    using intersection_entry = std::pair<std::uint32_t, std::uint32_t>;
    auto intersect = [&](auto &&lhs, auto &&rhs) {
        std::vector<intersection_entry> intersection;
        auto left = lhs->docs.begin();
        auto right = rhs->docs.begin();
        std::vector<std::uint32_t> documents(1);
        std::vector<std::uint32_t> frequencies(1);
        while (left != lhs->docs.end() && right != rhs->docs.end()) {
            if (*left == *right) {
                auto left_idx = std::distance(lhs->docs.begin(), left);
                auto right_idx = std::distance(rhs->docs.begin(), right);
                documents.push_back(*left);
                frequencies.push_back(lhs->freqs[left_idx]);
                frequencies.push_back(rhs->freqs[right_idx]);
                ++right;
                ++left;
            } else if (*left < *right) {
                ++left;
            } else {
                ++right;
            }
        }
        documents[0] = documents.size();
        frequencies[0] = frequencies.size();
        doc_stream.write(reinterpret_cast<char const *>(documents.data()), documents.size() * 4);
        freq_stream.write(reinterpret_cast<char const *>(frequencies.data()),
                          frequencies.size() * 4);
    };

    for (auto left_term = 0; left_term < input.size(); left_term += 1) {
        for (auto right_term = left_term + 1; right_term < input.size(); right_term += 1) {
            intersect(std::next(input.begin(), left_term), std::next(input.begin(), right_term));
            mapping.emplace_back(left_term, right_term);
        }
    }
    auto buffer =
        Payload_Vector_Buffer::make(mapping.begin(), mapping.end(), [](auto &&terms, auto output) {
            auto begin = reinterpret_cast<std::byte const *>(&terms.first);
            std::copy(begin, begin + 4, output);
            begin = reinterpret_cast<std::byte const *>(&terms.second);
            std::copy(begin, begin + 4, output);
        });
    buffer.to_file(fmt::format("{}.bimap", output_filename));
}

} // namespace pisa
