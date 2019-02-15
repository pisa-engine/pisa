#pragma once

#include <random>
#include <string>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <gsl/span>
#include <pstl/algorithm>
#include <pstl/execution>
#include <pstl/numeric>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/zip.hpp>
#include <spdlog/spdlog.h>
#include <tbb/task_scheduler_init.h>

#include "invert.hpp"
#include "io.hpp"
#include "type_safe.hpp"
#include "vector.hpp"

namespace pisa {

auto create_random_mapping(int document_count,
                           int shard_count,
                           std::optional<std::uint64_t> seed = std::nullopt)
    -> Vector<Document_Id, Shard_Id>
{
    namespace rv = ranges::view;
    std::vector<Document_Id> documents(document_count);
    std::iota(documents.begin(), documents.end(), Document_Id{});
    std::random_device rd;
    std::mt19937 g(seed.value_or(rd()));
    std::shuffle(documents.begin(), documents.end(), g);
    auto shard_size = ceil_div(document_count, shard_count);
    Vector<Document_Id, Shard_Id> mapping(document_count);
    for (auto &&[shard_id, shard] :
         rv::zip(rv::iota(Shard_Id{0u}, Shard_Id{shard_count}), rv::chunk(documents, shard_size)))
    {
        for (auto document : shard) {
            mapping[document] = shard_id;
        }
    }
    return mapping;
}

auto create_random_mapping(std::string const &input_basename,
                           int shard_count,
                           std::optional<std::uint64_t> seed = std::nullopt)
    -> Vector<Document_Id, Shard_Id>
{
    auto document_count = *(*binary_collection(input_basename.c_str()).begin()).begin();
    return create_random_mapping(document_count, shard_count, seed);
}

auto build_shard(std::string const &input_basename,
                 std::string const &output_basename,
                 Shard_Id shard_id,
                 std::vector<Document_Id> documents,
                 Vector<Term_Id, std::string> const &terms)
{
    auto fwd = binary_collection(input_basename.c_str());
    std::sort(documents.begin(), documents.end());

    std::ofstream os(fmt::format("{}.{:03d}", output_basename, shard_id.as_int()));

    auto document_count = static_cast<uint32_t>(documents.size());
    write_sequence(os, gsl::make_span<uint32_t const>(&document_count, 1));

    auto for_each_document_in_shard = [&](auto fn) {
        auto docid_iter = documents.begin();
        Document_Id current_document{0};

        for (auto fwd_iter = ++fwd.begin(); fwd_iter != fwd.end();
             std::advance(fwd_iter, 1), current_document += 1) {
            if (current_document != *docid_iter) {
                continue;
            }

            auto seq = *fwd_iter;
            fn(seq);

            std::advance(docid_iter, 1);
            if (docid_iter == documents.end()) {
                break;
            }
        }
    };

    std::vector<uint32_t> has_term(terms.size(), 0u);
    for_each_document_in_shard([&](auto const &sequence) {
        for (auto term : sequence) {
            has_term[term] = 1u;
        }
    });

    std::ofstream tos(fmt::format("{}.{:03d}.terms", output_basename, shard_id.as_int()));
    for (auto const &[term, occurs] : ranges::view::zip(terms, has_term)) {
        if (occurs) {
            tos << term << '\n';
        }
    }

    std::exclusive_scan(
        std::execution::seq, has_term.begin(), has_term.end(), has_term.begin(), 0u);
    auto remapped_term_id = [&](auto term) { return has_term[term]; };
    for_each_document_in_shard([&](auto const &sequence) {
        std::vector<uint32_t> terms(sequence.size());
        std::transform(sequence.begin(), sequence.end(), terms.begin(), remapped_term_id);
        write_sequence(os, gsl::span<uint32_t const>(&terms[0], terms.size()));
    });

    std::ifstream dis(fmt::format("{}.documents", input_basename));
    std::ofstream dos(fmt::format("{}.{:03d}.documents", output_basename, shard_id.as_int()));
    auto docid_iter = documents.begin();
    Document_Id current_document{0};
    pisa::io::for_each_line(dis, [&](std::string const & document_title) {
        if (docid_iter == documents.end() || current_document != *docid_iter) {
            current_document += 1;
            return;
        }
        dos << document_title << '\n';
        if (docid_iter != documents.end()) {
            std::advance(docid_iter, 1);
            current_document += 1;
        }
    });
}

auto partition_fwd_index(std::string const &input_basename,
                         std::string const &output_basename,
                         Vector<Document_Id, Shard_Id> &mapping)
{
    spdlog::info("Partitioning titles");
    auto terms = io::read_type_safe_string_vector<Term_Id>(fmt::format("{}.terms", input_basename));

    spdlog::info("Partitioning documents");
    std::vector<std::pair<Shard_Id, std::vector<Document_Id>>> shard_documents;
    {
        auto mapped_pairs = ranges::to_vector(mapping.entries());
        std::sort(std::execution::par_unseq,
                  mapped_pairs.begin(),
                  mapped_pairs.end(),
                  [](auto const &lhs, auto const &rhs) { return lhs.second < rhs.second; });
        auto pos = mapped_pairs.begin();
        while (pos != mapped_pairs.end()) {
            shard_documents.emplace_back(pos->second, std::vector<Document_Id>{});
            auto next = std::find_if(
                pos, mapped_pairs.end(), [&, shard_id = pos->second](auto const &entry) {
                    if (entry.second != shard_id) {
                        return true;
                    }
                    shard_documents.back().second.push_back(entry.first);
                    return false;
                });
            pos = next;
        };
    }

    std::for_each(
        //std::execution::par_unseq,
        shard_documents.begin(),
        shard_documents.end(),
        [&](auto &&entry) {
            build_shard(
                input_basename, output_basename, entry.first, std::move(entry.second), terms);
        });

    spdlog::info("Partitioning terms");
}

} // namespace pisa
