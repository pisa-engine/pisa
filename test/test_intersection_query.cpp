#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <bitset>
#include <tbb/task_scheduler_init.h>

#include "cursor/scored_cursor.hpp"
#include "cursor/span_cursor.hpp"
#include "cursor/union.hpp"
#include "in_memory_index.hpp"
#include "int_iter.hpp"
#include "pisa_config.hpp"
#include "query/algorithm/inter_query.hpp"
#include "query/algorithm/ranked_or_query.hpp"
#include "query/queries.hpp"
#include "scorer/bm25.hpp"

using namespace pisa;

template <typename Index>
struct IndexData {
    static std::unordered_map<std::string, std::unique_ptr<IndexData>> data;

    explicit IndexData(std::string const &scorer_name)
        : collection(PISA_SOURCE_DIR "/test/test_data/test_collection"),
          document_sizes(PISA_SOURCE_DIR "/test/test_data/test_collection.sizes"),
          wdata(document_sizes.begin()->begin(),
                collection.num_docs(),
                collection,
                scorer_name,
                BlockSize(FixedBlock()))

    {
        typename Index::builder builder(collection.num_docs(), params);
        for (auto const &plist : collection) {
            uint64_t freqs_sum =
                std::accumulate(plist.frequencies.begin(), plist.frequencies.end(), uint64_t(0));
            builder.add_posting_list(plist.documents.size(),
                                     plist.documents.begin(),
                                     plist.frequencies.begin(),
                                     freqs_sum);
        }
        builder.build(index);

        term_id_vec q;
        std::ifstream qfile(PISA_SOURCE_DIR "/test/test_data/queries");
        auto push_query = [&](std::string const &query_line) {
            queries.push_back(parse_query_ids(query_line));
        };
        io::for_each_line(qfile, push_query);

        std::string t;
    }

    [[nodiscard]] static auto get(std::string const &s_name)
    {
        if (IndexData::data.find(s_name) == IndexData::data.end()) {
            IndexData::data[s_name] = std::make_unique<IndexData<Index>>(s_name);
        }
        return IndexData::data[s_name].get();
    }

    global_parameters params;
    BinaryFreqCollection collection;
    BinaryCollection document_sizes;
    Index index;
    std::vector<Query> queries;
    wand_data<wand_data_raw> wdata;
};

template <typename Index>
std::unordered_map<std::string, unique_ptr<IndexData<Index>>> IndexData<Index>::data = {};

TEST_CASE("Extract IDs from a intersection bitset", "[intersection_query][unit]")
{
    REQUIRE(extract_indices({0b000}, 3) == std::vector<std::uint32_t>{});
    REQUIRE(extract_indices({0b001}, 3) == std::vector<std::uint32_t>{0});
    REQUIRE(extract_indices({0b010}, 3) == std::vector<std::uint32_t>{1});
    REQUIRE(extract_indices({0b100}, 3) == std::vector<std::uint32_t>{2});
    REQUIRE(extract_indices({0b011}, 3) == std::vector<std::uint32_t>{0, 1});
    REQUIRE(extract_indices({0b101}, 3) == std::vector<std::uint32_t>{0, 2});
    REQUIRE(extract_indices({0b110}, 3) == std::vector<std::uint32_t>{1, 2});
    REQUIRE(extract_indices({0b111}, 3) == std::vector<std::uint32_t>{0, 1, 2});
}

TEST_CASE("Safe intersections", "[inter_query][unit]")
{
    auto intersections =
        GENERATE(std::vector<std::bitset<64>>{{0b001}, {0b010}, {0b100}},
                 std::vector<std::bitset<64>>{{0b011}, {0b100}, {0b001}, {0b110}},
                 std::vector<std::bitset<64>>{
                     {0b001}, {0b010}, {0b100}, {0b011}, {0b110}, {0b101}, {0b111}});

    InMemoryIndex index{
        {{0, 2, 4, 6}, {0, 2, 10}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);
    bm25<InMemoryWand> scorer(wand);
    auto results = intersection_query(index, Query{{}, {0, 1, 2}, {}}, intersections, scorer, 10);
    std::sort(results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
        return lhs.first > rhs.first;
    });

    ranked_or_query q(10);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());
    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());

    REQUIRE(results == expected);
}

TEST_CASE("k = 2", "[inter_query][unit]")
{
    // Here, k = 2 because when using only bigram intersections, these are the only results
    // that will be returned; in other words, it's unsafe to call it with only bigrams
    // in this case.
    int k = 2;

    // The additional intersection of all three terms does not change anything.
    auto intersections = GENERATE(std::vector<std::bitset<64>>{{0b011}, {0b110}, {0b101}},
                                  std::vector<std::bitset<64>>{{0b011}, {0b110}, {0b101}, {0b111}});
    InMemoryIndex index{
        {{0, 2, 4, 6}, {1, 2, 3}, {2, 4, 10}}, {{1, 1, 1, 1}, {1, 1, 1}, {1, 1, 1}}, 100};
    InMemoryWand wand(index);
    bm25<InMemoryWand> scorer(wand);
    auto results = intersection_query(index, Query{{}, {0, 1, 2}, {}}, intersections, scorer, k);
    std::sort(results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
        return lhs.first > rhs.first;
    });

    ranked_or_query q(k);
    auto cursors = make_scored_cursors(index, scorer, Query{std::nullopt, {0, 1, 2}, {}});
    q(gsl::make_span(cursors), index.num_docs());
    std::vector<std::pair<float, std::uint64_t>> expected(q.topk().begin(), q.topk().end());

    REQUIRE(results == expected);
}

TEST_CASE("Execute on test index", "[inter_query][integration]")
{
    tbb::task_scheduler_init init(1);
    using make_inter_type = std::function<std::vector<std::bitset<64>>(Query)>;
    auto make_intersections =
        GENERATE(make_inter_type([](auto query) {
                     std::vector<std::bitset<64>> intersections;
                     std::transform(iter<std::size_t>(0),
                                    iter(query.terms.size()),
                                    std::back_inserter(intersections),
                                    [](auto term_idx) { return std::bitset<64>(1 << term_idx); });
                     return intersections;
                 }),
                 make_inter_type([](auto query) {
                     std::vector<std::bitset<64>> intersections;
                     std::transform(iter<std::size_t>(0),
                                    iter(query.terms.size()),
                                    std::back_inserter(intersections),
                                    [](auto term_idx) { return std::bitset<64>(1 << term_idx); });
                     std::transform(iter<std::size_t>(1),
                                    iter(query.terms.size()),
                                    std::back_inserter(intersections),
                                    [](auto n) { return std::bitset<64>(0b11 << n); });
                     return intersections;
                 }),
                 make_inter_type([](auto query) {
                     std::vector<std::bitset<64>> intersections;
                     std::transform(iter<std::size_t>(1),
                                    iter(std::size_t(1) << query.terms.size()),
                                    std::back_inserter(intersections),
                                    [](auto n) { return std::bitset<64>(n); });
                     return intersections;
                 }));
    for (auto &&scorer_name : {"bm25"}) {
        auto data = IndexData<single_index>::get(scorer_name);
        ranked_or_query or_q(10);

        with_scorer(scorer_name, data->wdata, [&](auto scorer) {
            for (auto const &q : data->queries) {
                auto cursors = make_scored_cursors(data->index, scorer, q);
                or_q(gsl::make_span(cursors), data->index.num_docs());
                auto results =
                    intersection_query(data->index, q, make_intersections(q), scorer, 10);
                REQUIRE(or_q.topk().size() == results.size());
                for (size_t i = 0; i < or_q.topk().size(); ++i) {
                    CAPTURE(i);
                    REQUIRE(or_q.topk()[i].first == Approx(results[i].first).epsilon(0.1));
                }
            }
        });
    }
}

TEST_CASE("Resolving terms with intersections", "[inter_query][unit]")
{
    auto [input_query, input_inters, expected_query, expected_inters] =
        GENERATE(table<Query, std::vector<std::bitset<64>>, Query, std::vector<std::bitset<64>>>(
            {{Query{{}, {0, 1, 2}, {}},
              {{0b001}, {0b010}, {0b100}},
              Query{{}, {0, 1, 2}, {}},
              {{0b001}, {0b010}, {0b100}}},
             {Query{{}, {0, 1, 0}, {}},
              {{0b001}, {0b010}, {0b100}},
              Query{{}, {0, 1}, {}},
              {{0b001}, {0b010}}}}));
    CAPTURE(input_query.terms);
    CAPTURE(input_inters);
    resolve(input_query, input_inters);
    CHECK(input_query.terms == expected_query.terms);
    CHECK(input_inters == expected_inters);
}

TEST_CASE("Remap intersections", "[inter_query][unit]")
{
    auto [mapping, input_inters, expected_inters] =
        GENERATE(table<std::vector<std::optional<std::size_t>>,
                       std::vector<std::bitset<64>>,
                       std::vector<std::bitset<64>>>(
            {{{0, 1, 2}, {{0b001}, {0b010}, {0b100}}, {{0b001}, {0b010}, {0b100}}},
             {{2, 0, 1}, {{0b001}, {0b010}, {0b100}}, {{0b100}, {0b001}, {0b010}}},
             {{2, 0, std::nullopt}, {{0b001}, {0b010}, {0b100}}, {{0b100}, {0b001}}}}));
    CAPTURE(mapping);
    CAPTURE(input_inters);
    remap_intersections(input_inters, mapping);
    REQUIRE(input_inters == expected_inters);
}
