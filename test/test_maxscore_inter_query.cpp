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
#include "query/algorithm/maxscore_inter_eager_query.hpp"
#include "query/algorithm/maxscore_inter_opt_query.hpp"
#include "query/algorithm/maxscore_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
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

        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries.selections.jl");
        auto push_query = [&](std::string const& query_line) {
            queries.push_back(QueryContainer::from_json(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::vector<TermPair> pairs;
        for (auto&& query: queries) {
            auto request = query.query(pisa::query::unlimited);
            auto term_ids = request.term_ids();
            for (auto left = 0; left < term_ids.size(); left += 1) {
                for (auto right = left + 1; right < term_ids.size(); right += 1) {
                    pairs.emplace_back(term_ids[left], term_ids[right]);
                }
            }
        }

        build_binary_index(compressed_path.string(), std::move(pairs), binary_index_path.string());
        pair_index = std::make_unique<pisa::PairIndex<binary_index_type>>(
            pisa::PairIndex<binary_index_type>::load(binary_index_path.string()));
    }

    [[nodiscard]] static auto get(std::string const& s_name)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData>(s_name);
        }
        return IndexData::data[s_name].get();
    }

    TemporaryDirectory tmp{};
    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<QueryContainer> queries;
    wand_data<wand_data_raw> wdata;
    std::unique_ptr<pisa::PairIndex<binary_index_type>> pair_index = nullptr;
};

std::unordered_map<std::string, unique_ptr<IndexData>> IndexData::data = {};

std::ostream& operator<<(std::ostream& os, std::array<std::uint32_t, 2> p)
{
    os << "(" << p[0] << ", " << p[1] << ")";
    return os;
}

// NOLINTNEXTLINE(hicpp-explicit-conversions)
TEMPLATE_TEST_CASE(
    "Ranked query test",
    "[query][ranked][integration]",
    maxscore_inter_eager_query,
    maxscore_inter_opt_query)
{
    std::unordered_set<size_t> dropped_term_ids;
    auto data = IndexData::get("bm25");
    topk_queue baseline_topk(10);
    ranked_or_query baseline(baseline_topk);
    topk_queue topk(10);
    TestType algo(topk);

    auto scorer = scorer::from_params(ScorerParams("bm25"), data->wdata);
    std::cout << std::setprecision(std::numeric_limits<float>::digits10 + 1);
    std::cerr << std::setprecision(std::numeric_limits<float>::digits10 + 1);

    SECTION("All single terms")
    {
        for (auto& q: data->queries) {
            CAPTURE(*q.term_ids());
            /* Select all single terms and no pairs. */
            std::vector<std::size_t> term_positions(q.term_ids()->size());
            std::iota(term_positions.begin(), term_positions.end(), 0);
            q.add_selection(10, {.selected_terms = term_positions, .selected_pairs = {}});

            baseline(make_scored_cursors(data->index, *scorer, q.query(10)), data->index.num_docs());
            baseline_topk.finalize();
            algo(
                q.query(10, RequestFlagSet::all()),
                data->index,
                data->wdata,
                *data->pair_index,
                *scorer,
                data->index.num_docs());
            topk.finalize();
            REQUIRE(topk.topk().size() == baseline_topk.topk().size());
            for (size_t i = 0; i < baseline_topk.topk().size(); ++i) {
                CAPTURE(i);
                REQUIRE(baseline_topk.topk()[i].first == Approx(topk.topk()[i].first).epsilon(0.1));
            }
            topk.clear();
            baseline_topk.clear();
        }
    }

    SECTION("All possible intersections: single and pairs")
    {
        int idx = 0;
        for (auto& q: data->queries) {
            CAPTURE(idx++);
            /* Select all single terms. */
            std::vector<std::size_t> term_positions(q.term_ids()->size());
            std::iota(term_positions.begin(), term_positions.end(), 0);
            /* Select all pairs. */
            std::vector<std::array<std::size_t, 2>> pairs;
            auto term_ids = *q.term_ids();
            for (std::size_t left = 0; left < term_ids.size(); left += 1) {
                for (std::size_t right = left + 1; right < term_ids.size(); right += 1) {
                    if (data->pair_index->pair_id(term_ids[left], term_ids[right])) {
                        pairs.push_back({left, right});
                    }
                }
            }

            q.add_selection(10, {.selected_terms = term_positions, .selected_pairs = pairs});

            baseline(make_scored_cursors(data->index, *scorer, q.query(10)), data->index.num_docs());
            baseline_topk.finalize();
            algo(
                q.query(10, RequestFlagSet::all()),
                data->index,
                data->wdata,
                *data->pair_index,
                *scorer,
                data->index.num_docs());
            topk.finalize();
            REQUIRE(topk.topk().size() == baseline_topk.topk().size());
            for (size_t i = 0; i < baseline_topk.topk().size(); ++i) {
                REQUIRE(baseline_topk.topk()[i].first == Approx(topk.topk()[i].first).epsilon(0.1));
            }
            topk.clear();
            baseline_topk.clear();
        }
    }

    SECTION("Optimized selections (from input file)")
    {
        for (auto q: data->queries) {
            baseline(make_scored_cursors(data->index, *scorer, q.query(10)), data->index.num_docs());
            baseline_topk.finalize();
            algo(
                q.query(10, RequestFlagSet::all()),
                data->index,
                data->wdata,
                *data->pair_index,
                *scorer,
                data->index.num_docs());
            topk.finalize();
            REQUIRE(topk.topk().size() == baseline_topk.topk().size());
            for (size_t i = 0; i < baseline_topk.topk().size(); ++i) {
                REQUIRE(baseline_topk.topk()[i].first == Approx(topk.topk()[i].first).epsilon(0.1));
            }
            topk.clear();
            baseline_topk.clear();
        }
    }
}
