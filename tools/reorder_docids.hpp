#pragma once

#include <algorithm>
#include <fstream>

#include <gsl/span>

#include "app.hpp"
#include "binary_freq_collection.hpp"
#include "recursive_graph_bisection.hpp"
#include "util/index_build_utils.hpp"
#include "util/inverted_index_utils.hpp"
#include "util/progress.hpp"

namespace pisa {

auto reorder_postings(
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

auto reorder_lexicon(
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

auto reorder_sizes(
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

void reorder_from_mapping(
    binary_freq_collection const& input_collection,
    binary_collection const& input_sizes,
    std::string output_basename,
    std::optional<std::string> const& document_lexicon,
    std::optional<std::string> const& reordered_document_lexicon,
    gsl::span<std::uint32_t const> mapping)
{
    auto num_docs = input_collection.num_docs();
    reorder_sizes(input_sizes, num_docs, mapping, output_basename);
    reorder_postings(input_collection, output_basename, mapping);
    if (document_lexicon) {
        reorder_lexicon(*document_lexicon, *reordered_document_lexicon, mapping);
    }
}

auto reorder_random(
    std::string const& input_basename,
    std::string output_basename,
    std::optional<std::string> const& document_lexicon,
    std::optional<std::string> const& reordered_document_lexicon,
    std::uint64_t seed) -> int
{
    spdlog::info("Computing random permutation");
    binary_freq_collection input_collection(input_basename.c_str());
    auto num_docs = input_collection.num_docs();
    std::vector<std::uint32_t> mapping(num_docs);
    std::iota(mapping.begin(), mapping.end(), uint32_t());
    std::shuffle(mapping.begin(), mapping.end(), std::mt19937{seed});

    binary_collection input_sizes(fmt::format("{}.sizes", input_basename).c_str());
    reorder_from_mapping(
        input_collection,
        input_sizes,
        output_basename,
        document_lexicon,
        reordered_document_lexicon,
        mapping);
    return 0;
}

auto reorder_url(
    std::string const& input_basename,
    std::string output_basename,
    std::optional<std::string> const& document_lexicon,
    std::optional<std::string> const& reordered_document_lexicon,
    std::string const& url_file) -> int
{
    spdlog::info("Sorting URLs");
    binary_freq_collection input_collection(input_basename.c_str());
    auto const mapping = [&] {
        std::vector<std::pair<std::string, std::uint32_t>> urls;
        urls.reserve(input_collection.num_docs());
        std::ifstream is(url_file);
        std::string url;
        std::size_t count = 0;
        while (std::getline(is, url)) {
            urls.emplace_back(std::move(url), count);
            ++count;
        }
        if (input_collection.num_docs() != count) {
            throw std::invalid_argument(fmt::format("Invalid URL file: {}", url_file));
        }
        std::sort(urls.begin(), urls.end());
        std::vector<std::uint32_t> mapping(urls.size());
        std::transform(urls.begin(), urls.end(), mapping.begin(), [](auto&& p) { return p.second; });
        return mapping;
    }();

    binary_collection input_sizes(fmt::format("{}.sizes", input_basename).c_str());
    reorder_from_mapping(
        input_collection,
        input_sizes,
        output_basename,
        document_lexicon,
        reordered_document_lexicon,
        mapping);
    return 0;
}

auto reorder_from_input(
    std::string const& input_basename,
    std::string output_basename,
    std::optional<std::string> const& document_lexicon,
    std::optional<std::string> const& reordered_document_lexicon,
    std::string mapping_file) -> int
{
    spdlog::info("Reading mapping");
    binary_freq_collection input_collection(input_basename.c_str());
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
    binary_collection input_sizes(fmt::format("{}.sizes", input_basename).c_str());
    reorder_from_mapping(
        input_collection,
        input_sizes,
        output_basename,
        document_lexicon,
        reordered_document_lexicon,
        mapping);
    return 0;
}

inline auto reorder_bp(
    std::string const& input_basename,
    std::optional<std::string> const& output_basename,
    std::optional<std::string> const& document_lexicon,
    std::optional<std::string> const& reordered_document_lexicon,
    std::optional<std::string> const& input_fwd,
    std::optional<std::string> const& output_fwd,
    std::optional<std::string> const& node_config,
    std::size_t min_length,
    std::optional<std::size_t> depth,
    bool nogb,
    bool print) -> int
{
    if (not output_basename && not output_fwd) {
        spdlog::error("Must define at least one output parameter.");
        return 1;
    }

    forward_index fwd = input_fwd
        ? forward_index::read(*input_fwd)
        : forward_index::from_inverted_index(input_basename, min_length, not nogb);

    if (output_fwd) {
        forward_index::write(fwd, *output_fwd);
    }

    if (output_basename) {
        std::vector<uint32_t> documents(fwd.size());
        std::iota(documents.begin(), documents.end(), 0U);
        std::vector<double> gains(fwd.size(), 0.0);
        bp::range_type initial_range(documents.begin(), documents.end(), fwd, gains);

        if (node_config) {
            bp::run_with_config(*node_config, initial_range);
        } else {
            bp::run_default_tree(
                depth.value_or(static_cast<size_t>(std::log2(fwd.size()) - 5)), initial_range);
        }

        if (print) {
            for (const auto& document: documents) {
                std::cout << document << '\n';
            }
        }
        auto mapping = get_mapping(documents);
        fwd.clear();
        documents.clear();
        reorder_inverted_index(input_basename, *output_basename, mapping);

        if (document_lexicon) {
            auto doc_buffer = Payload_Vector_Buffer::from_file(*document_lexicon);
            auto documents = Payload_Vector<std::string>(doc_buffer);
            std::vector<std::string> reordered_documents(documents.size());
            pisa::progress doc_reorder("Reordering documents vector", documents.size());
            for (size_t i = 0; i < documents.size(); ++i) {
                reordered_documents[mapping[i]] = documents[i];
                doc_reorder.update(1);
            }
            encode_payload_vector(reordered_documents.begin(), reordered_documents.end())
                .to_file(*reordered_document_lexicon);
        }
    }
    return 0;
}

auto reorder_docids(ReorderDocuments args) -> int
{
    try {
        if (args.random()) {
            return reorder_random(
                args.input_basename(),
                *args.output_basename(),
                args.document_lexicon(),
                args.reordered_document_lexicon(),
                args.seed());
        }
        if (auto url_file = args.url_file(); url_file) {
            return reorder_url(
                args.input_basename(),
                *args.output_basename(),
                args.document_lexicon(),
                args.reordered_document_lexicon(),
                *url_file);
        }
        if (auto input = args.order_file(); input) {
            return reorder_from_input(
                args.input_basename(),
                *args.output_basename(),
                args.document_lexicon(),
                args.reordered_document_lexicon(),
                *input);
        }
        if (args.bp()) {
            return reorder_bp(
                args.input_basename(),
                args.output_basename(),
                args.document_lexicon(),
                args.reordered_document_lexicon(),
                args.input_fwd(),
                args.output_fwd(),
                args.node_config(),
                args.min_length(),
                args.depth(),
                args.nogb(),
                args.print());
        }
    } catch (std::invalid_argument& err) {
        spdlog::error("{}", err.what());
        std::exit(1);
    }
    spdlog::error("Should be unreachable due to argument constraints!");
    std::exit(1);
}

}  // namespace pisa
