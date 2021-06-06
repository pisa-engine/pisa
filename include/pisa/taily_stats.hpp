#pragma once

#include <iostream>
#include <optional>

#include <gsl/span>
#include <mio/mmap.hpp>
#include <taily.hpp>

#include "binary_freq_collection.hpp"
#include "memory_source.hpp"
#include "query/queries.hpp"
#include "scorer/scorer.hpp"
#include "timer.hpp"
#include "type_safe.hpp"
#include "util/progress.hpp"
#include "vec_map.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

class TailyStats {
  public:
    explicit TailyStats(MemorySource source) : m_source(std::move(source)) {}

    static auto from_mapped(std::string const& path) -> TailyStats
    {
        return TailyStats(MemorySource::mapped_file(path));
    }

    [[nodiscard]] auto num_documents() const -> std::uint64_t { return read_at<std::uint64_t>(0); }
    [[nodiscard]] auto num_terms() const -> std::uint64_t { return read_at<std::uint64_t>(8); }
    [[nodiscard]] auto term_stats(term_id_type term_id) const -> taily::Feature_Statistics
    {
        std::size_t offset = 16 + term_id * 24;
        auto expected_value = read_at<double>(offset);
        auto variance = read_at<double>(offset + sizeof(double));
        auto frequency = read_at<std::int64_t>(offset + 2 * sizeof(double));
        return taily::Feature_Statistics{expected_value, variance, frequency};
    }
    [[nodiscard]] auto query_stats(pisa::Query const& query) const -> taily::Query_Statistics
    {
        std::vector<taily::Feature_Statistics> stats;
        std::transform(
            query.terms.begin(), query.terms.end(), std::back_inserter(stats), [this](auto&& term_id) {
                return this->term_stats(term_id);
            });
        return taily::Query_Statistics{std::move(stats), static_cast<std::int64_t>(num_documents())};
    }

  private:
    template <typename T>
    [[nodiscard]] PISA_ALWAYSINLINE auto read_at(std::size_t pos) const -> T
    {
        static_assert(std::is_pod<T>::value, "The value type must be POD.");
        T value{};
        auto bytes = this->bytes(pos, sizeof(T));
        std::memcpy(&value, bytes.data(), sizeof(T));
        return value;
    }

    [[nodiscard]] PISA_ALWAYSINLINE auto bytes(std::size_t start, std::size_t size) const
        -> gsl::span<char const>
    {
        try {
            return m_source.subspan(start, size);
        } catch (std::out_of_range const&) {
            throw std::out_of_range(fmt::format(
                "Tried to read bytes {}-{} but memory source is of size {}",
                start,
                start + size,
                m_source.size()));
        }
    }

    MemorySource m_source;
};

/// Constructs a vector of `taily::Feature_Statistics` for each term in the `collection` using
/// `scorer`.
template <typename Scorer>
[[nodiscard]] auto extract_feature_stats(pisa::binary_freq_collection const& collection, Scorer scorer)
    -> std::vector<taily::Feature_Statistics>
{
    std::vector<taily::Feature_Statistics> term_stats;
    {
        pisa::progress progress("Processing posting lists", collection.size());
        std::size_t term_id = 0;
        for (auto const& seq: collection) {
            std::vector<float> scores;
            std::uint64_t size = seq.docs.size();
            scores.reserve(size);
            auto term_scorer = scorer->term_scorer(term_id);
            for (std::size_t i = 0; i < seq.docs.size(); ++i) {
                std::uint64_t docid = *(seq.docs.begin() + i);
                std::uint64_t freq = *(seq.freqs.begin() + i);
                float score = term_scorer(docid, freq);
                scores.push_back(score);
            }
            term_stats.push_back(taily::Feature_Statistics::from_features(scores));
            term_id += 1;
            progress.update(1);
        }
    }
    return term_stats;
}

void write_feature_stats(
    gsl::span<taily::Feature_Statistics> stats,
    std::size_t num_documents,
    std::string const& output_path)
{
    std::ofstream ofs(output_path);
    ofs.write(reinterpret_cast<const char*>(&num_documents), sizeof(num_documents));
    std::size_t num_terms = stats.size();
    ofs.write(reinterpret_cast<const char*>(&num_terms), sizeof(num_terms));
    for (auto&& ts: stats) {
        ts.to_stream(ofs);
    }
}

/// For each query, call `func` with a vector of shard scores.
template <typename Fn>
void taily_score_shards(
    std::string const& global_stats_path,
    VecMap<Shard_Id, std::string> const& shard_stats_paths,
    std::vector<::pisa::Query> const& global_queries,
    VecMap<Shard_Id, std::vector<::pisa::Query>> const& shard_queries,
    std::size_t k,
    Fn func)
{
    if (shard_stats_paths.size() != shard_queries.size()) {
        throw std::invalid_argument(fmt::format(
            "Number of discovered shard stats paths ({}) does not match number of "
            "parsed query lists ({})",
            shard_stats_paths.size(),
            shard_queries.size()));
    }
    std::for_each(shard_queries.begin(), shard_queries.end(), [&global_queries](auto&& sq) {
        if (global_queries.size() != sq.size()) {
            throw std::invalid_argument(
                "Global queries and shard queries do not all have the same size.");
        }
    });

    auto global_stats = pisa::TailyStats::from_mapped(global_stats_path);
    std::vector<pisa::TailyStats> shard_stats;
    std::transform(
        shard_stats_paths.begin(),
        shard_stats_paths.end(),
        std::back_inserter(shard_stats),
        [](auto&& path) { return pisa::TailyStats::from_mapped(path); });
    for (std::size_t query_idx = 0; query_idx < global_queries.size(); query_idx += 1) {
        auto global = global_stats.query_stats(global_queries[query_idx]);
        std::vector<taily::Query_Statistics> shards;
        std::transform(
            shard_stats.begin(),
            shard_stats.end(),
            shard_queries.begin(),
            std::back_inserter(shards),
            [query_idx](
                auto&& shard, auto&& queries) { return shard.query_stats(queries[query_idx]); });
        auto [scores, time] = run_with_timer_ret<std::chrono::microseconds>(
            [&] { return taily::score_shards(global, shards, k); });
        func(scores, time);
    }
}

}  // namespace pisa
