#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fmt/format.h>
#include <gsl/span>
#include <pisa/cursor/scored_cursor.hpp>
#include <pisa/intersection.hpp>
#include <pisa/io.hpp>
#include <rapidcheck.h>

#include "pisa_config.hpp"
#include "test_common.hpp"

using namespace pisa;
using namespace pisa::intersection;

TEST_CASE("filter query", "[intersection][unit]")
{
    GIVEN("With term IDs")
    {
        auto query = QueryContainer::from_term_ids({6, 1, 5});
        auto [mask, expected] = GENERATE(table<Mask, QueryContainer>({
            {0b001, QueryContainer::from_term_ids({6})},
            {0b010, QueryContainer::from_term_ids({1})},
            {0b100, QueryContainer::from_term_ids({5})},
            {0b011, QueryContainer::from_term_ids({6, 1})},
            {0b101, QueryContainer::from_term_ids({6, 5})},
            {0b110, QueryContainer::from_term_ids({1, 5})},
            {0b111, QueryContainer::from_term_ids({6, 1, 5})},
        }));
        WHEN("Filtered with mask " << mask)
        {
            auto actual = filter(query, mask);
            CHECK(actual.term_ids() == expected.term_ids());
            CHECK(actual.terms() == expected.terms());
        }
    }
    GIVEN("With terms")
    {
        auto query = QueryContainer::from_terms({"a", "b", "c"}, std::nullopt);
        auto [mask, expected] = GENERATE(table<Mask, QueryContainer>({
            {0b001, QueryContainer::from_terms({"a"}, std::nullopt)},
            {0b010, QueryContainer::from_terms({"b"}, std::nullopt)},
            {0b100, QueryContainer::from_terms({"c"}, std::nullopt)},
            {0b011, QueryContainer::from_terms({"a", "b"}, std::nullopt)},
            {0b101, QueryContainer::from_terms({"a", "c"}, std::nullopt)},
            {0b110, QueryContainer::from_terms({"b", "c"}, std::nullopt)},
            {0b111, QueryContainer::from_terms({"a", "b", "c"}, std::nullopt)},
        }));
        WHEN("Filtered with mask " << mask)
        {
            auto actual = filter(query, mask);
            REQUIRE(actual.term_ids() == expected.term_ids());
            REQUIRE(*actual.terms() == *expected.terms());
        }
    }
}

struct VectorCursor {
    gsl::span<std::uint32_t const> documents;
    gsl::span<std::uint32_t const> frequencies;
    std::uint32_t max_docid;

    std::array<std::uint32_t, 1> sentinel_document;

    [[nodiscard]] auto size() const noexcept -> std::size_t { return documents.size(); }
    [[nodiscard]] auto docid() const noexcept -> std::uint32_t { return documents[0]; }
    [[nodiscard]] auto freq() const noexcept -> std::uint32_t { return frequencies[0]; }
    void next()
    {
        if (documents[0] < max_docid) {
            documents = documents.subspan(1);
            frequencies = frequencies.subspan(1);
            try_finish();
        }
    }
    void next_geq(std::uint32_t docid)
    {
        if (documents[0] < max_docid) {
            auto new_pos = std::lower_bound(documents.begin(), documents.end(), docid);
            auto skip = std::distance(documents.begin(), new_pos);
            documents = documents.subspan(skip);
            frequencies = frequencies.subspan(skip);
            try_finish();
        }
    }

  private:
    void try_finish()
    {
        if (documents.empty()) {
            documents = gsl::make_span(sentinel_document);
        }
    }
};

struct InMemoryIndex {
    using document_enumerator = VectorCursor;

    std::vector<std::vector<std::uint32_t>> documents;
    std::vector<std::vector<std::uint32_t>> frequencies;
    std::uint32_t num_documents;

    [[nodiscard]] auto operator[](std::uint32_t term_id) const -> VectorCursor
    {
        if (term_id >= size()) {
            throw std::out_of_range(
                fmt::format("Term {} is out of range; index contains {} terms", term_id, size()));
        }
        return {
            gsl::make_span(documents[term_id]),
            gsl::make_span(frequencies[term_id]),
            num_documents,
            {num_documents}};
    }

    [[nodiscard]] auto size() const noexcept -> std::size_t { return documents.size(); }
    [[nodiscard]] auto num_docs() const noexcept -> std::size_t { return num_documents; }
};

struct InMemoryWand {
    std::vector<float> max_weights;
    std::uint32_t num_documents;

    [[nodiscard]] auto max_term_weight(std::uint32_t term_id) const noexcept -> float
    {
        return max_weights[term_id];
    }
    [[nodiscard]] auto term_posting_count(std::uint32_t term_id) const noexcept { return 1; }
    [[nodiscard]] auto term_occurrence_count(std::uint32_t term_id) const noexcept { return 1; }

    [[nodiscard]] auto norm_len(std::uint32_t docid) const noexcept { return 1.0; }
    [[nodiscard]] auto doc_len(std::uint32_t docid) const noexcept { return 1; }
    [[nodiscard]] auto avg_len() const noexcept { return 1.0; }
    [[nodiscard]] auto num_docs() const noexcept -> std::size_t { return num_documents; }
    [[nodiscard]] auto collection_len() const noexcept -> std::size_t { return 1; }
};

TEST_CASE("Vector cursor", "[intersection][unit]")
{
    std::vector<std::uint32_t> documents{0, 3, 5, 6, 87, 111};
    std::vector<std::uint32_t> frequencies{1, 4, 6, 7, 88, 112};

    auto cursor = VectorCursor{gsl::make_span(documents), gsl::make_span(frequencies), 200, {200}};

    REQUIRE(cursor.size() == 6);

    REQUIRE(cursor.docid() == 0);
    REQUIRE(cursor.freq() == 1);

    cursor.next();
    REQUIRE(cursor.docid() == 3);
    REQUIRE(cursor.freq() == 4);

    cursor.next();
    REQUIRE(cursor.docid() == 5);
    REQUIRE(cursor.freq() == 6);

    cursor.next();
    REQUIRE(cursor.docid() == 6);
    REQUIRE(cursor.freq() == 7);

    cursor.next();
    REQUIRE(cursor.docid() == 87);
    REQUIRE(cursor.freq() == 88);

    cursor.next();
    REQUIRE(cursor.docid() == 111);
    REQUIRE(cursor.freq() == 112);

    cursor.next();
    REQUIRE(cursor.docid() == 200);

    cursor.next();
    REQUIRE(cursor.docid() == 200);

    // NEXTGEQ
    cursor = VectorCursor{gsl::make_span(documents), gsl::make_span(frequencies), 200, {200}};

    REQUIRE(cursor.docid() == 0);
    REQUIRE(cursor.freq() == 1);

    cursor.next_geq(4);
    REQUIRE(cursor.docid() == 5);
    REQUIRE(cursor.freq() == 6);

    cursor.next_geq(87);
    REQUIRE(cursor.docid() == 87);
    REQUIRE(cursor.freq() == 88);

    cursor.next_geq(178);
    REQUIRE(cursor.docid() == 200);
}

TEST_CASE("compute intersection", "[intersection][unit]")
{
    GIVEN("Four-term query, index, and wand data object")
    {
        InMemoryIndex index{
            {
                {0},  // 0
                {0, 1, 2},  // 1
                {0},  // 2
                {0},  // 3
                {0},  // 4
                {0, 1, 4},  // 5
                {1, 4, 8},  // 6
            },
            {
                {1},  // 0
                {1, 1, 1},  // 1
                {1},  // 2
                {1},  // 3
                {1},  // 4
                {1, 1, 1},  // 5
                {1, 1, 1},  // 6
            },
            10};
        InMemoryWand wand{{0.0, 1.0, 0.0, 0.0, 0.0, 5.0, 6.0}, 10};

        auto query = QueryContainer::from_term_ids({6, 1, 5});
        auto [mask, len, max] = GENERATE(table<Mask, std::size_t, float>({
            {0b001, 3, 1.84583f},
            {0b010, 3, 1.84583f},
            {0b100, 3, 1.84583f},
            {0b011, 1, 3.69165f},
            {0b101, 2, 3.69165f},
            {0b110, 2, 3.69165f},
            {0b111, 1, 5.53748f},
        }));
        WHEN("Computed intersection with mask " << mask)
        {
            auto intersection = Intersection::compute(index, wand, query, ScorerParams("bm25"), mask);
            CHECK(intersection.length == len);
            CHECK(intersection.max_score == Approx(max));
        }
    }
}

TEST_CASE("for_all_subsets", "[intersection][unit]")
{
    GIVEN("A query and a mock function that accumulates arguments")
    {
        std::vector<Mask> masks;
        auto accumulate = [&](QueryContainer const&, Mask const& mask) { masks.push_back(mask); };
        auto query = QueryContainer::from_term_ids({6, 1, 5});
        WHEN("Executed with limit 0")
        {
            for_all_subsets(query, 0, accumulate);
            THEN("No elements accumulated") { CHECK(masks.empty()); }
        }
        WHEN("Executed with limit 1")
        {
            for_all_subsets(query, 1, accumulate);
            THEN("Unigrams accumulated")
            {
                CHECK(masks == std::vector<Mask>{Mask(0b001), Mask(0b010), Mask(0b100)});
            }
        }
        WHEN("Executed with limit 2")
        {
            for_all_subsets(query, 2, accumulate);
            THEN("Unigrams and bigrams accumulated")
            {
                CHECK(
                    masks
                    == std::vector<Mask>{
                        Mask(0b001), Mask(0b010), Mask(0b011), Mask(0b100), Mask(0b101), Mask(0b110)});
            }
        }
        WHEN("Executed with limit 3")
        {
            for_all_subsets(query, 3, accumulate);
            THEN("All combinations accumulated")
            {
                CHECK(
                    masks
                    == std::vector<Mask>{
                        Mask(0b001),
                        Mask(0b010),
                        Mask(0b011),
                        Mask(0b100),
                        Mask(0b101),
                        Mask(0b110),
                        Mask(0b111)});
            }
        }
    }
}

struct IndexData {
    using index_type = block_simdbp_index;
    using binary_index_type = block_freq_index<simdbp_block, false, IndexArity::Binary>;

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

    TemporaryDirectory tmp{};
    global_parameters params;
    binary_freq_collection collection;
    binary_collection document_sizes;
    index_type index;
    std::vector<QueryContainer> queries;
    wand_data<wand_data_raw> wdata;
    std::unique_ptr<pisa::PairIndex<binary_index_type>> pair_index = nullptr;
};

TEST_CASE("Construct lattice -- single always present", "[intersection][prop]")
{
    using S = std::uint16_t;
    IndexData data("bm25");
    int idx = 0;
    for (auto query: data.queries) {
        CAPTURE(idx++);
        auto request = query.query(10);
        auto lattice =
            pisa::IntersectionLattice<S>::build(request, data.index, data.wdata, *data.pair_index);
        REQUIRE(lattice.query_length() == request.term_ids().size());
        auto singles = lattice.single_term_lists();
        std::vector<S> expected(request.term_ids().size());
        std::generate_n(
            expected.begin(), request.term_ids().size(), [idx = std::uint16_t(0)]() mutable {
                return 1 << idx++;
            });
        REQUIRE(std::vector<S>(singles.begin(), singles.end()) == expected);
        for (auto single: lattice.single_term_lists()) {
            REQUIRE(lattice.cost(single) < std::numeric_limits<std::uint32_t>::max());
        }
        for (std::uint32_t inter = 1; inter < std::uint32_t(1) << request.term_ids().size(); ++inter) {
            REQUIRE(lattice.score_bound(static_cast<S>(inter)) > 0);
        }
    }
}

TEST_CASE("Pascals triangle", "[intersection][unit]")
{
    auto [count, expected] = GENERATE(table<std::size_t, std::vector<std::uint32_t>>(
        {{1, {1}}, {2, {2, 1}}, {3, {3, 3, 1}}, {4, {4, 6, 4, 1}}, {7, {7, 21, 35, 35, 21, 7, 1}}}));
    auto span = PASCAL_TRIANGLE.intersection_counts(count);
    REQUIRE(std::vector<std::uint32_t>(span.begin(), span.end()) == expected);
}

TEST_CASE("Pascals triangle offsets", "[intersection][unit]")
{
    auto [count, expected] = GENERATE(table<std::size_t, std::vector<std::uint32_t>>(
        {{1, {0}}, {2, {0, 2}}, {3, {0, 3, 6}}, {4, {0, 4, 10, 14}}, {7, {0, 7, 28, 63, 98, 119, 126}}}));
    auto span = PASCAL_TRIANGLE.intersection_count_partial_sum(count);
    REQUIRE(std::vector<std::uint32_t>(span.begin(), span.end()) == expected);
}

TEST_CASE("Layered nodes", "[intersection][unit]")
{
    auto [lattice, expected] =
        GENERATE(table<pisa::IntersectionLattice<std::uint8_t>, std::vector<std::uint8_t>>(
            {{pisa::IntersectionLattice<std::uint8_t>({0b1}, {}, {}, {}), std::vector<std::uint8_t>{1}},
             {pisa::IntersectionLattice<std::uint8_t>({0b01, 0b10}, {}, {}, {}),
              std::vector<std::uint8_t>{1, 2, 3}},
             {pisa::IntersectionLattice<std::uint8_t>({0b001, 0b010, 0b100}, {}, {}, {}),
              std::vector<std::uint8_t>{1, 2, 4, 3, 5, 6, 7}},
             {pisa::IntersectionLattice<std::uint8_t>({0b0001, 0b0010, 0b0100, 0b1000}, {}, {}, {}),
              std::vector<std::uint8_t>{1, 2, 4, 8, 3, 5, 6, 9, 10, 12, 7, 11, 13, 14, 15}},
             {pisa::IntersectionLattice<std::uint8_t>(
                  {0b00001, 0b00010, 0b00100, 0b01000, 0b10000}, {}, {}, {}),
              std::vector<std::uint8_t>{1,  2,  4,  8,  16, 3,  5,  6,  9,  10, 12,
                                        17, 18, 20, 24, 7,  11, 13, 14, 19, 21, 22,
                                        25, 26, 28, 15, 23, 27, 29, 30, 31}}}));
    auto nodes = lattice.layered_nodes();
    REQUIRE(
        std::vector<std::uint8_t>(
            nodes.begin(), std::next(nodes.begin(), (1U << lattice.query_length()) - 1))
        == expected);
}

TEST_CASE("Candidates", "[intersection][unit]")
{
    using S = std::uint16_t;
    constexpr static auto max_subset_count =
        static_cast<std::size_t>(std::numeric_limits<S>::max()) + 1;
    SECTION("Fake query")
    {
        std::array<std::uint32_t, max_subset_count> costs{};
        std::array<float, max_subset_count> score_bounds{};
        score_bounds[0b0001] = 0.0;
        score_bounds[0b0010] = 1.0;
        score_bounds[0b0100] = 2.0;
        score_bounds[0b1000] = 3.0;
        score_bounds[0b0011] = 2.0;
        score_bounds[0b0101] = 3.0;
        score_bounds[0b1001] = 4.0;
        score_bounds[0b0110] = 4.0;
        score_bounds[0b1010] = 5.0;
        score_bounds[0b1100] = 6.0;
        pisa::IntersectionLattice<S> lattice(
            {0b0001, 0b0010, 0b0100, 0b1000},
            {0b0011, 0b0101, 0b1001, 0b0110, 0b1010, 0b1100},
            costs,
            score_bounds);
        auto [threshold, expected] = GENERATE(table<float, pisa::SelectionCandidtes<std::uint16_t>>(
            {{0.0, {{0b0001, 0b0010, 0b0100, 0b1000}, {0b0001, 0b0010, 0b0100, 0b1000}}},
             {1.0, {{0b0001, 0b0010, 0b0100, 0b1000}, {0b0010, 0b0100, 0b1000}}},
             {2.0, {{0b0001, 0b0010, 0b0100, 0b1000, 0b0011}, {0b0100, 0b1000, 0b0011}}}}));

        auto candidates = lattice.selection_candidates(threshold);
        REQUIRE(candidates.elements == expected.elements);
        REQUIRE(candidates.subsets == expected.subsets);
    }
    SECTION("Test queries")
    {
        using lattice_type = pisa::IntersectionLattice<S>;
        auto [lattice, threshold, elements, subsets] = GENERATE(
            []() {
                // school unified district downey
                std::array<float, max_subset_count> score_bounds{};
                std::array<std::uint32_t, max_subset_count> costs{};
                score_bounds[0b0001] = 5.653356075286865;
                score_bounds[0b0010] = 12.529928207397461;
                score_bounds[0b0100] = 3.252655029296875;
                score_bounds[0b1000] = 9.169288635253906;
                lattice_type lattice(
                    {0b0001, 0b0010, 0b0100, 0b1000}, {0b0101, 0b1001, 0b1100}, costs, score_bounds);
                lattice.calc_remaining_score_bounds();
                return std::make_tuple(
                    std::move(lattice),
                    16.043301,
                    std::vector<S>{0b0011, 0b1010, 0b1101},
                    std::vector<S>{0b0001, 0b0010, 0b0100, 0b1000, 0b0101, 0b1001, 0b1100});
            }(),
            []() {
                // buses pittsburgh
                std::array<float, max_subset_count> score_bounds{};
                std::array<std::uint32_t, max_subset_count> costs{};
                score_bounds[0b01] = 13.052966117858888;
                score_bounds[0b10] = 8.719024658203125;
                score_bounds[0b11] = 19.0411434173584;
                lattice_type lattice({0b01, 0b10}, {0b11}, costs, score_bounds);
                lattice.calc_remaining_score_bounds();
                return std::make_tuple(
                    std::move(lattice), 10.110200, std::vector<S>{0b01}, std::vector<S>{0b10, 0b01});
            }(),
            []() {
                // party slumber bears
                std::array<float, max_subset_count> score_bounds{};
                std::array<std::uint32_t, max_subset_count> costs{};
                score_bounds[0b001] = 6.039915561676025;
                score_bounds[0b010] = 4.34684419631958;
                score_bounds[0b100] = 13.127897262573242;
                score_bounds[0b101] = 18.312904357910156;
                score_bounds[0b110] = 17.45214080810547;
                lattice_type lattice({0b001, 0b010, 0b100}, {0b101, 0b110}, costs, score_bounds);
                lattice.calc_remaining_score_bounds();
                return std::make_tuple(
                    std::move(lattice),
                    14.999400,
                    std::vector<S>{0b101, 0b110},
                    std::vector<S>{0b001, 0b010, 0b100, 0b101, 0b110});
            }(),
            []() {
                // wine food pairing
                std::array<float, max_subset_count> score_bounds{};
                std::array<std::uint32_t, max_subset_count> costs{};
                score_bounds[0b001] = 3.946219682693481;
                score_bounds[0b010] = 7.167963981628418;
                score_bounds[0b100] = 6.829254150390625;
                score_bounds[0b110] = 10.726018905639648;
                lattice_type lattice({0b001, 0b010, 0b100}, {0b101, 0b110}, costs, score_bounds);
                lattice.calc_remaining_score_bounds();
                return std::make_tuple(
                    std::move(lattice),
                    15.075,
                    std::vector<S>{0b111},
                    std::vector<S>{0b001, 0b010, 0b100, 0b110, 0b101});
            }(),
            []() {
                std::array<float, max_subset_count> score_bounds{};
                std::array<std::uint32_t, max_subset_count> costs{};
                score_bounds[0b00001] = 6.591092;
                score_bounds[0b00010] = 7.099102;
                score_bounds[0b00100] = 8.421594;
                score_bounds[0b01000] = 5.883558;
                score_bounds[0b10000] = 3.896044;
                // pairs: (3:13.6902:9) (5:15.01269:11) (9:12.47465:49)
                // (17:10.48714:81) (6:15.5207:15) (10:12.98266:13) (18:10.99515:31) (12:14.30515:8)
                // (20:12.31764:32) (24:9.779603:91)
                lattice_type lattice(
                    {0b00001, 0b00010, 0b00100, 0b01000, 0b10000},
                    {0b00011,
                     0b00101,
                     0b01001,
                     0b10001,
                     0b00110,  //
                     0b01010,
                     0b10010,
                     0b01100,
                     0b10100,
                     0b11000},
                    costs,
                    score_bounds);
                lattice.calc_remaining_score_bounds();
                return std::make_tuple(
                    std::move(lattice),
                    12.71845,
                    std::vector<S>{0b00011, 0b00101, 0b00110, 0b01010, 0b01100, 0b11001},
                    std::vector<S>{
                        0b00001,
                        0b00010,
                        0b00100,
                        0b01000,
                        0b10000,
                        0b00011,
                        0b00101,
                        0b00110,
                        0b01010,
                        0b01100,
                        0b01001,
                        0b10001,
                        0b10010,
                        0b10100,
                        0b11000});
            }());
        //        Loaded: |  (32804,83459) (32804,87398) (32804,89621) (83459,87398) (83459,89621)
        //        (87398,89621)
        // t: 12.71845
        // Computed: |  (32804,83459) (32804,87398) (83459,87398) (83459,89621)
        // (87398,89621)
        // 8
        //

        auto candidates = lattice.selection_candidates(threshold);
        std::sort(candidates.elements.begin(), candidates.elements.end());
        std::sort(candidates.subsets.begin(), candidates.subsets.end());
        std::sort(elements.begin(), elements.end());
        std::sort(subsets.begin(), subsets.end());
        CHECK(candidates.elements == elements);
        CHECK(candidates.subsets == subsets);
    }
}

TEST_CASE("Solve", "[intersection][unit]")
{
    using S = std::uint16_t;
    constexpr static auto max_subset_count =
        static_cast<std::size_t>(std::numeric_limits<S>::max()) + 1;
    using lattice_type = pisa::IntersectionLattice<S>;
    auto [lattice, threshold, expected] = GENERATE(
        []() {
            // school unified district downey
            std::array<float, max_subset_count> score_bounds{};
            std::array<std::uint32_t, max_subset_count> costs{};
            costs[0b0001] = 2423488;
            costs[0b0010] = 67622;
            costs[0b0100] = 7660019;
            costs[0b1000] = 395014;
            costs[0b0101] = 935805;
            costs[0b1001] = 72728;
            costs[0b1100] = 133514;
            score_bounds[0b0001] = 5.653356075286865;
            score_bounds[0b0010] = 12.529928207397461;
            score_bounds[0b0100] = 3.252655029296875;
            score_bounds[0b1000] = 9.169288635253906;
            lattice_type lattice(
                {0b0001, 0b0010, 0b0100, 0b1000}, {0b0101, 0b1001, 0b1100}, costs, score_bounds);
            lattice.calc_remaining_score_bounds();
            return std::make_tuple(
                std::move(lattice), 16.043301, pisa::Selected<S>{{0b0010, 0b1001}, 140350});
        }(),
        []() {
            // buses pittsburgh
            std::array<float, max_subset_count> score_bounds{};
            std::array<std::uint32_t, max_subset_count> costs{};
            costs[0b01] = 46969;
            costs[0b10] = 500508;
            costs[0b11] = 1435;
            score_bounds[0b01] = 13.052966117858888;
            score_bounds[0b10] = 8.719024658203125;
            score_bounds[0b11] = 19.0411434173584;
            lattice_type lattice({0b01, 0b10}, {0b11}, costs, score_bounds);
            lattice.calc_remaining_score_bounds();
            return std::make_tuple(std::move(lattice), 10.110200, pisa::Selected<S>{{1}, 46969});
        }(),
        []() {
            // party slumber bears
            std::array<float, max_subset_count> score_bounds{};
            std::array<std::uint32_t, max_subset_count> costs{};
            costs[0b001] = 1994671;
            costs[0b010] = 4606787;
            costs[0b100] = 46983;
            costs[0b101] = 11059;
            costs[0b110] = 20953;
            score_bounds[0b001] = 6.039915561676025;
            score_bounds[0b010] = 4.34684419631958;
            score_bounds[0b100] = 13.127897262573242;
            score_bounds[0b101] = 18.312904357910156;
            score_bounds[0b110] = 17.45214080810547;
            lattice_type lattice({0b001, 0b010, 0b100}, {0b101, 0b110}, costs, score_bounds);
            lattice.calc_remaining_score_bounds();
            return std::make_tuple(
                std::move(lattice), 14.999400, pisa::Selected<S>{{0b101, 0b110}, 32012});
        }(),
        []() {
            // wine food pairing
            std::array<float, max_subset_count> score_bounds{};
            std::array<std::uint32_t, max_subset_count> costs{};
            costs[0b001] = 5569437;
            costs[0b010] = 1118862;
            costs[0b100] = 1334056;
            costs[0b101] = 651602;
            score_bounds[0b001] = 3.946219682693481;
            score_bounds[0b010] = 7.167963981628418;
            score_bounds[0b100] = 6.829254150390625;
            score_bounds[0b110] = 10.726018905639648;
            lattice_type lattice({0b001, 0b010, 0b100}, {0b101}, costs, score_bounds);
            lattice.calc_remaining_score_bounds();
            return std::make_tuple(std::move(lattice), 15.075, pisa::Selected<S>{{0b101}, 651602});
        }());

    auto candidates = lattice.selection_candidates(threshold);
    auto selected = candidates.solve(lattice.costs());
    CHECK(selected.intersections == expected.intersections);
    /* CHECK(candidates.subsets == subsets); */
}
