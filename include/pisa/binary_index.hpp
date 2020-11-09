#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>
#include <unordered_map>

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
#include "util/verify_collection.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

template <typename Index>
class PairIndex {
  public:
    using index_type = Index;
    using cursor_type = typename Index::document_enumerator;

    [[nodiscard]] static auto load(std::string const& file_path, bool disk_resident = true)
        -> PairIndex
    {
        try {
            auto source = [&]() {
                if (disk_resident) {
                    return MemorySource::disk_resident_file(file_path);
                }
                return MemorySource::mapped_file(file_path);
            }();
            PairIndex<Index> pair_index(Index(std::move(source)));

            pair_index.m_mapping_source =
                mio::mmap_source(fmt::format("{}.pairs", file_path).c_str());
            mapper::map(pair_index.m_pair_mapping, pair_index.m_mapping_source);

            pair_index.m_posting_counts_source =
                mio::mmap_source(fmt::format("{}.postingcounts", file_path).c_str());
            mapper::map(pair_index.m_pair_posting_counts, pair_index.m_posting_counts_source);

            return pair_index;
        } catch (std::system_error const& err) {
            throw std::runtime_error(
                fmt::format("Failed to load pair index from {}: {}", file_path, err.what()));
        }
    }

    [[nodiscard]] auto index() -> Index& { return m_index; }
    [[nodiscard]] auto index() const -> Index const& { return m_index; }
    [[nodiscard]] auto pair_id(TermId left, TermId right) const -> std::optional<TermId>
    {
        auto pair_entry = TermPair{left, right};
        auto pos = std::lower_bound(m_pair_mapping.begin(), m_pair_mapping.end(), pair_entry);
        if (pos != m_pair_mapping.end() && *pos == pair_entry) {
            return static_cast<TermId>(std::distance(m_pair_mapping.begin(), pos));
        }
        return std::nullopt;
    }
    [[nodiscard]] auto pair_posting_count(TermId pair_id) const -> std::uint32_t
    {
        return m_pair_posting_counts.at(pair_id);
    }
    [[nodiscard]] auto pair_posting_count(TermId left, TermId right) const -> std::uint32_t
    {
        return pair_posting_count(TermPair{left, right});
    }

  private:
    explicit PairIndex(Index index) : m_index(std::move(index)) {}

    Index m_index;
    mio::mmap_source m_mapping_source{};
    mio::mmap_source m_posting_counts_source{};
    mapper::mappable_vector<TermPair> m_pair_mapping{};
    mapper::mappable_vector<std::uint32_t> m_pair_posting_counts{};
};

template <typename Index, typename BinaryBuilder>
void build_binary_index(
    Index const& index,
    BinaryBuilder builder,
    std::string const& output_filename,
    std::vector<TermPair> pairs)
{
    using namespace pisa;
    spdlog::info("Building {} pairs", pairs.size());
    double tick = get_time_usecs();

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());

    std::vector<TermPair> pair_mapping;
    pair_mapping.reserve(pairs.size());
    std::vector<std::uint32_t> posting_counts;
    posting_counts.reserve(pairs.size());

    size_t postings = 0;
    {
        pisa::progress progress("Create index", pairs.size());
        for (auto [left_term, right_term]: pairs) {
            std::vector<typename Index::document_enumerator> cursors{
                index[left_term], index[right_term]};
            auto intersection = intersect(
                number_cursors(cursors),
                std::array<std::uint32_t, 2>{0, 0},
                [](auto frequencies, auto&& cursor) {
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
            posting_counts.push_back(size);
            pair_mapping.emplace_back(left_term, right_term);
            builder.add_posting_list(
                size, documents.begin(), frequencies.begin(), 0  // unused for block index
            );
            progress.update(1);
            postings += size;
        }
    }

    std::cerr << "Flushing metadata...\n";
    builder.build(output_filename.c_str());

    std::cerr << "Writing pair mapping...\n";
    mapper::mappable_vector<TermPair> mappable_pair_mapping;
    mappable_pair_mapping.steal(pair_mapping);
    mapper::freeze(mappable_pair_mapping, fmt::format("{}.pairs", output_filename).c_str());

    std::cerr << "Writing posting counts...\n";
    mapper::mappable_vector<std::uint32_t> mappable_posting_counts;
    mappable_posting_counts.steal(posting_counts);
    mapper::freeze(mappable_posting_counts, fmt::format("{}.postingcounts", output_filename).c_str());
}

void build_binary_index(
    std::string const& index_filename, std::vector<TermPair> pairs, std::string const& output_filename)
{
    using binary_index_type = block_freq_index<pisa::simdbp_block, false, IndexArity::Binary>;
    block_simdbp_index index(MemorySource::mapped_file(index_filename));
    spdlog::info("Loading index from {}", index_filename);

    build_binary_index(
        index,
        typename binary_index_type::stream_builder(index.num_docs(), pisa::global_parameters{}),
        output_filename,
        std::move(pairs));
}

}  // namespace pisa
