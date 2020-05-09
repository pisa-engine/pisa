#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "configuration.hpp"
#include "cursor/cursor_intersection.hpp"
#include "cursor/numbered_cursor.hpp"
#include "index_types.hpp"
#include "linear_quantizer.hpp"
#include "mappable/mapper.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp"  // XXX move to index_build_utils
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

template <typename Index, typename Wand, typename BinaryBuilder, typename BinaryIndex>
void build_binary_index(
    Index const& index,
    Wand const& wdata,
    BinaryBuilder builder,
    BinaryIndex binary_index,
    std::string const& output_filename,
    std::vector<std::array<std::uint32_t, 2>> pairs)
{
    using namespace pisa;
    spdlog::info("Building {} pairs", pairs.size());
    double tick = get_time_usecs();

    for (auto& p: pairs) {
        if (std::get<0>(p) > std::get<0>(p)) {
            std::swap(std::get<0>(p), std::get<0>(p));
        }
    }
    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
    std::vector<std::array<std::uint32_t, 2>> pair_mapping;

    size_t postings = 0;
    {
        pisa::progress progress("Create index", pairs.size());
        for (auto [left_term, right_term]: pairs) {
            std::vector<typename Index::document_enumerator> cursors{
                index[left_term], index[right_term]};
            auto intersection = intersect(
                number_cursors(cursors),
                std::array<std::uint32_t, 2>{0, 0},
                [](auto& frequencies, auto&& cursor) {
                    frequencies[cursor.term_position()] = cursor.freq();
                    return frequencies;
                });
            std::vector<std::uint32_t> documents;
            std::vector<std::array<std::uint32_t, 2>> frequencies;
            while (not intersection.empty()) {
                documents.push_back(intersection.docid());
                frequencies.push_back(intersection.payload());
                intersection.next();
            }
            std::size_t size = documents.size();
            if (size == 0) {
                continue;
            }
            pair_mapping.push_back({left_term, right_term});
            builder.add_posting_list(
                size, documents.begin(), frequencies.begin(), /* unused for block inde */ 0);
            progress.update(1);
            postings += size;
        }
    }

    builder.build(binary_index);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("Collection built in {} seconds", elapsed_secs);
    mapper::freeze(binary_index, output_filename.c_str());
    mapper::mappable_vector<std::array<std::uint32_t, 2>> mappable_pair_mapping;
    mappable_pair_mapping.steal(pair_mapping);
    mapper::freeze(mappable_pair_mapping, fmt::format("{}.pairs", output_filename).c_str());
}

void build_binary_index(
    std::string const& index_filename,
    std::string const& wand_data_filename,
    std::vector<std::array<std::uint32_t, 2>> pairs,
    std::string const& output_filename)
{
    using binary_index_type = block_freq_index<pisa::simdbp_block, false, IndexArity::Binary>;
    block_simdbp_index index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    wand_data<wand_data_raw> wdata;
    mio::mmap_source md(wand_data_filename);
    mapper::map(wdata, md, mapper::map_flags::warmup);

    build_binary_index(
        index,
        wdata,
        typename binary_index_type::builder(index.num_docs(), pisa::global_parameters{}),
        binary_index_type{},
        output_filename,
        std::move(pairs));
}

}  // namespace pisa
