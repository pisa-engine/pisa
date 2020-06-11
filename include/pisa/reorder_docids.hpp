#pragma once

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include <gsl/span>
#include <spdlog/spdlog.h>

#include "binary_freq_collection.hpp"
#include "recursive_graph_bisection.hpp"
#include "util/index_build_utils.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/progress.hpp"

namespace pisa {

struct RecursiveGraphBisectionOptions {
    std::string input_basename;
    std::optional<std::string> output_basename;
    std::optional<std::string> output_fwd;
    std::optional<std::string> input_fwd;
    std::optional<std::string> document_lexicon;
    std::optional<std::string> reordered_document_lexicon;
    std::optional<std::size_t> depth;
    std::optional<std::string> node_config;
    std::size_t min_length;
    bool compress_fwd;
    bool print_args;
};

namespace detail {
    using iterator_type = std::vector<uint32_t>::iterator;
    using range_type = document_range<iterator_type>;
    using node_type = computation_node<iterator_type>;

    inline std::vector<node_type>
    read_node_config(const std::string& config_file, const range_type& initial_range)
    {
        std::vector<node_type> nodes;
        std::ifstream is(config_file);
        std::string line;
        while (std::getline(is, line)) {
            std::istringstream iss(line);
            nodes.push_back(node_type::from_stream(iss, initial_range));
        }
        return nodes;
    }

    inline void run_with_config(const std::string& config_file, const range_type& initial_range)
    {
        auto nodes = read_node_config(config_file, initial_range);
        auto total_count = std::accumulate(
            nodes.begin(), nodes.end(), std::ptrdiff_t(0), [](auto acc, const auto& node) {
                return acc + node.partition.size();
            });
        pisa::progress bp_progress("Graph bisection", total_count);
        bp_progress.update(0);
        recursive_graph_bisection(std::move(nodes), bp_progress);
    }

    inline void run_default_tree(size_t depth, const range_type& initial_range)
    {
        spdlog::info("Default tree with depth {}", depth);
        pisa::progress bp_progress("Graph bisection", initial_range.size() * depth);
        bp_progress.update(0);
        recursive_graph_bisection(initial_range, depth, depth - 6, bp_progress);
    }

}  // namespace detail

[[nodiscard]] auto recursive_graph_bisection(RecursiveGraphBisectionOptions const& options) -> int
{
    if (not options.output_basename && not options.output_fwd) {
        spdlog::error("Must define at least one output parameter.");
        return 1;
    }

    forward_index fwd = options.input_fwd
        ? forward_index::read(*options.input_fwd)
        : forward_index::from_inverted_index(
            options.input_basename, options.min_length, options.compress_fwd);

    if (options.output_fwd) {
        forward_index::write(fwd, *options.output_fwd);
    }

    if (options.output_basename) {
        std::vector<uint32_t> documents(fwd.size());
        std::iota(documents.begin(), documents.end(), 0U);
        std::vector<double> gains(fwd.size(), 0.0);
        detail::range_type initial_range(documents.begin(), documents.end(), fwd, gains);

        if (options.node_config) {
            detail::run_with_config(*options.node_config, initial_range);
        } else {
            detail::run_default_tree(
                options.depth.value_or(static_cast<size_t>(std::log2(fwd.size()) - 5)),
                initial_range);
        }

        if (options.print_args) {
            for (const auto& document: documents) {
                std::cout << document << '\n';
            }
        }
        auto mapping = get_mapping(documents);
        fwd.clear();
        documents.clear();
        reorder_inverted_index(options.input_basename, *options.output_basename, mapping);

        if (options.document_lexicon) {
            auto doc_buffer = Payload_Vector_Buffer::from_file(*options.document_lexicon);
            auto documents = Payload_Vector<std::string>(doc_buffer);
            std::vector<std::string> reordered_documents(documents.size());
            pisa::progress doc_reorder("Reordering documents vector", documents.size());
            for (size_t i = 0; i < documents.size(); ++i) {
                reordered_documents[mapping[i]] = documents[i];
                doc_reorder.update(1);
            }
            encode_payload_vector(reordered_documents.begin(), reordered_documents.end())
                .to_file(*options.reordered_document_lexicon);
        }
    }
    return 0;
}

struct ReorderOptions {
    std::string input_basename;
    std::string output_basename;
    std::optional<std::string> document_lexicon;
    std::optional<std::string> reordered_document_lexicon;
};

inline auto reorder_postings(
    binary_freq_collection const& input,
    std::string_view output_basename,
    gsl::span<std::uint32_t const> mapping)
{
    pisa::progress progress("Reassigning IDs in posting lists", input.size());

    std::ofstream output_docs(fmt::format("{}.docs", output_basename));
    std::ofstream output_freqs(fmt::format("{}.freqs", output_basename));
    emit(output_docs, 1);
    emit(output_docs, input.num_docs());

    std::vector<std::pair<std::uint32_t, std::uint32_t>> posting_list;
    for (const auto& seq: input) {
        for (size_t i = 0; i < seq.docs.size(); ++i) {
            posting_list.emplace_back(mapping[seq.docs.begin()[i]], seq.freqs.begin()[i]);
        }

        std::sort(posting_list.begin(), posting_list.end());

        emit(output_docs, posting_list.size());
        emit(output_freqs, posting_list.size());
        for (const auto& posting: posting_list) {
            emit(output_docs, posting.first);
            emit(output_freqs, posting.second);
        }

        progress.update(1);
        posting_list.clear();
    }
}

inline auto reorder_lexicon(
    std::string const& input_lexicon,
    std::string const& output_lexicon,
    gsl::span<std::uint32_t const> mapping)

{
    auto doc_buffer = Payload_Vector_Buffer::from_file(input_lexicon);
    auto documents = Payload_Vector<std::string>(doc_buffer);
    std::vector<std::string> reordered_documents(documents.size());
    pisa::progress doc_reorder("Reordering documents vector", documents.size());
    for (size_t i = 0; i < documents.size(); ++i) {
        reordered_documents[mapping[i]] = documents[i];
        doc_reorder.update(1);
    }
    encode_payload_vector(reordered_documents.begin(), reordered_documents.end()).to_file(output_lexicon);
}

inline auto reorder_sizes(
    binary_collection const& input_sizes,
    std::uint64_t num_docs,
    gsl::span<std::uint32_t const> mapping,
    std::string_view output_basename)
{
    pisa::progress progress("Reordering document sizes", num_docs);
    auto sizes = *input_sizes.begin();
    if (sizes.size() != num_docs) {
        throw std::invalid_argument("Invalid sizes file");
    }

    auto size_sequence = gsl::span(sizes.begin(), sizes.size());
    std::vector<std::uint32_t> new_sizes(num_docs);
    for (size_t i = 0; i < num_docs; ++i) {
        new_sizes[mapping[i]] = size_sequence[i];
        progress.update(1);
    }

    std::ofstream output_sizes(fmt::format("{}.sizes", output_basename));
    emit(output_sizes, new_sizes.size());
    emit(output_sizes, new_sizes.data(), num_docs);
}

inline void reorder_from_mapping(
    binary_freq_collection const& input_collection,
    binary_collection const& input_sizes,
    ReorderOptions const& options,
    gsl::span<std::uint32_t const> mapping)
{
    auto num_docs = input_collection.num_docs();
    reorder_sizes(input_sizes, num_docs, mapping, options.output_basename);
    reorder_postings(input_collection, options.output_basename, mapping);
    if (options.document_lexicon) {
        reorder_lexicon(*options.document_lexicon, *options.reordered_document_lexicon, mapping);
    }
}

inline auto reorder_random(ReorderOptions options, unsigned int seed) -> int
{
    spdlog::info("Computing random permutation");
    binary_freq_collection input_collection(options.input_basename.c_str());
    auto num_docs = input_collection.num_docs();
    std::vector<std::uint32_t> mapping(num_docs);
    std::iota(mapping.begin(), mapping.end(), uint32_t());
    std::shuffle(mapping.begin(), mapping.end(), std::mt19937{seed});

    binary_collection input_sizes(fmt::format("{}.sizes", options.input_basename).c_str());
    reorder_from_mapping(input_collection, input_sizes, options, mapping);
    return 0;
}

/// Feature file must contain the same number of labels as there are documents in the collection.
inline auto reorder_by_feature(ReorderOptions options, std::string const& feature_file) -> int
{
    spdlog::info("Sorting URLs");
    binary_freq_collection input_collection(options.input_basename.c_str());
    auto const mapping = [&] {
        std::vector<std::pair<std::string, std::uint32_t>> urls;
        urls.reserve(input_collection.num_docs());
        std::ifstream is(feature_file);
        std::string url;
        std::size_t count = 0;
        while (std::getline(is, url)) {
            urls.emplace_back(std::move(url), count);
            ++count;
        }
        if (input_collection.num_docs() != count) {
            throw std::invalid_argument(fmt::format("Invalid URL file: {}", feature_file));
        }
        std::sort(urls.begin(), urls.end());
        std::vector<std::uint32_t> mapping(urls.size());
        std::transform(urls.begin(), urls.end(), mapping.begin(), [](auto&& p) { return p.second; });
        return mapping;
    }();

    binary_collection input_sizes(fmt::format("{}.sizes", options.input_basename).c_str());
    reorder_from_mapping(input_collection, input_sizes, options, mapping);
    return 0;
}

inline auto reorder_from_mapping(ReorderOptions options, std::string mapping_file) -> int
{
    spdlog::info("Reading mapping");
    binary_freq_collection input_collection(options.input_basename.c_str());
    auto const mapping = [&] {
        std::vector<std::uint32_t> mapping(input_collection.num_docs());

        std::ifstream is(mapping_file);
        uint32_t prev_id, new_id;
        size_t count = 0;
        while (is >> prev_id >> new_id) {
            mapping[prev_id] = new_id;
            ++count;
        }
        if (mapping.size() != count) {
            throw std::invalid_argument(fmt::format("Invalid document order file: {}", mapping_file));
        }
        return mapping;
    }();
    binary_collection input_sizes(fmt::format("{}.sizes", options.input_basename).c_str());
    reorder_from_mapping(input_collection, input_sizes, options, mapping);
    return 0;
}

}  // namespace pisa
