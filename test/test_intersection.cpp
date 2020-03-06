#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fmt/format.h>
#include <gsl/span>

#include "cursor/scored_cursor.hpp"
#include "intersection.hpp"

using namespace pisa;
using namespace pisa::intersection;

TEST_CASE("filter query", "[intersection][unit]")
{
    GIVEN("Four-term query")
    {
        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
        auto [mask, expected] = GENERATE(table<Mask, Query>({
            {0b001, Query{"Q1", {6}, {0.1}}},
            {0b010, Query{"Q1", {1}, {0.4}}},
            {0b100, Query{"Q1", {5}, {1.0}}},
            {0b011, Query{"Q1", {6, 1}, {0.1, 0.4}}},
            {0b101, Query{"Q1", {6, 5}, {0.1, 1.0}}},
            {0b110, Query{"Q1", {1, 5}, {0.4, 1.0}}},
            {0b111, Query{"Q1", {6, 1, 5}, {0.1, 0.4, 1.0}}},
        }));
        WHEN("Filtered with mask " << mask)
        {
            auto actual = filter(query, mask);
            CHECK(actual.id == expected.id);
            CHECK(actual.terms == expected.terms);
            CHECK(actual.term_weights == expected.term_weights);
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
    [[nodiscard]] auto freq() const noexcept -> float { return frequencies[0]; }
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
        return {gsl::make_span(documents[term_id]),
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
        InMemoryIndex index{{
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

        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
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
            auto intersection = Intersection::compute(index, wand, query, mask);
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
        auto accumulate = [&](Query const&, Mask const& mask) { masks.push_back(mask); };
        Query query{
            "Q1",  // query ID
            {6, 1, 5},  // terms
            {0.1, 0.4, 1.0}  // weights
        };
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
                    == std::vector<Mask>{Mask(0b001),
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
