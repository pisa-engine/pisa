#define CATCH_CONFIG_MAIN

#include <array>
#include <catch2/catch.hpp>
#include <functional>

#include <fmt/format.h>
#include <mio/mmap.hpp>

#include "accumulator/lazy_accumulator.hpp"
#include "binary_index.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "pisa_config.hpp"
#include "query.hpp"
#include "query/algorithm.hpp"
#include "temporary_directory.hpp"
#include "test_common.hpp"

using namespace pisa;

struct IndexData {
    using index_type = block_simdbp_index;
    using binary_index_type = block_freq_index<simdbp_block, false, IndexArity::Binary>;

    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    explicit IndexData(std::string const& scorer_name)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(
              document_sizes.begin()->begin(),
              collection.num_docs(),
              collection,
              ScorerParams(scorer_name),
              BlockSize(FixedBlock(5)),
              false,
              {})

    {
        typename index_type::builder builder(collection.num_docs(), params);
        for (auto const& plist: collection) {
            uint64_t freqs_sum = std::accumulate(plist.freqs.begin(), plist.freqs.end(), uint64_t(0));
            builder.add_posting_list(
                plist.docs.size(), plist.docs.begin(), plist.freqs.begin(), freqs_sum);
        }

        auto compressed_path = (tmp.path() / "compressed");
        auto wdata_path = (tmp.path() / "bmw");
        auto binary_index_path = (tmp.path() / "binary");

        builder.build(index);

        mapper::freeze(index, compressed_path.c_str());
        mapper::freeze(wdata, wdata_path.c_str());

        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries.jl");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(QueryContainer::from_json(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::vector<TermPair> pairs;
        for (auto&& query: queries) {
            auto const& term_ids = *query.term_ids();
            for (auto left = 0; left < term_ids.size(); left += 1) {
                for (auto right = left + 1; right < term_ids.size(); right += 1) {
                    if (term_ids[left] != term_ids[right]) {
                        pairs.emplace_back(term_ids[left], term_ids[right]);
                    }
                }
            }
        }

        build_binary_index(compressed_path.string(), std::move(pairs), binary_index_path.string());
        binary_index =
            std::make_unique<binary_index_type>(MemorySource::mapped_file(binary_index_path));
        pair_mapping_source =
            mio::mmap_source(fmt::format("{}.pairs", binary_index_path.string()).c_str());
        mapper::map(pair_mapping, pair_mapping_source.data());
    }

    [[nodiscard]] static auto get(std::string const& s_name)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData>(s_name);
        }
        return IndexData::data[s_name].get();
    }

    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<QueryContainer> queries;
    wand_data<wand_data_raw> wdata;
    TemporaryDirectory tmp;
    // mio::mmap_source binary_index_source;
    std::unique_ptr<binary_index_type> binary_index;
    mio::mmap_source pair_mapping_source;
    mapper::mappable_vector<TermPair> pair_mapping;
};

std::unordered_map<std::string, unique_ptr<IndexData>> IndexData::data = {};

std::ostream& operator<<(std::ostream& os, std::array<std::uint32_t, 2> p)
{
    os << "(" << p[0] << ", " << p[1] << ")";
    return os;
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEST_CASE("Ranked query test", "[query][ranked][integration]")
{
    for (auto quantized: {false, true}) {
        auto data = IndexData::get("bm25");
        scored_and_query andq;
        auto scorer = scorer::from_params(ScorerParams("bm25"), data->wdata);
        for (auto&& query: data->queries) {
            auto const& term_ids = *query.term_ids();
            for (std::uint32_t left = 0; left < term_ids.size(); left += 1) {
                for (std::uint32_t right = left + 1; right < term_ids.size(); right += 1) {
                    if (term_ids[left] != term_ids[right]) {
                        auto expected = andq(
                            make_scored_cursors(
                                data->index,
                                *scorer,
                                QueryContainer::from_term_ids({term_ids[left], term_ids[right]})
                                    .query(query::unlimited)),
                            data->index.num_docs());
                        if (!expected.empty()) {
                            auto term_pair = pisa::TermPair(term_ids[left], term_ids[right]);
                            auto pos = std::lower_bound(
                                data->pair_mapping.begin(), data->pair_mapping.end(), term_pair);
                            REQUIRE(pos != data->pair_mapping.end());
                            REQUIRE(*pos == term_pair);
                            auto pair_id = std::distance(data->pair_mapping.begin(), pos);
                            auto cursor = (*data->binary_index)[pair_id];
                            std::vector<std::pair<std::uint32_t, float>> actual;
                            auto left_scorer = scorer->term_scorer(term_pair.front());
                            auto right_scorer = scorer->term_scorer(term_pair.back());
                            while (cursor.docid() < cursor.universe()) {
                                auto score = left_scorer(cursor.docid(), std::get<0>(cursor.freq()));
                                score += right_scorer(cursor.docid(), std::get<1>(cursor.freq()));
                                actual.emplace_back(cursor.docid(), score);
                                cursor.next();
                            }
                            REQUIRE(actual.size() == expected.size());
                            for (auto idx = 0; idx < actual.size(); ++idx) {
                                CAPTURE(idx);
                                REQUIRE(actual[idx].first == expected[idx].first);
                                REQUIRE(actual[idx].second == Approx(expected[idx].second));
                            }
                        }
                    }
                }
            }
        }
    }
}
