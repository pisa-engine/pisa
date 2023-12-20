#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "pisa/scorer/index_scorer.hpp"
#include "pisa/scorer/scorer.hpp"

using namespace pisa;

struct WandData {
    [[nodiscard]] auto term_posting_count(std::uint32_t term_id) const -> std::size_t {
        switch (term_id) {
        case 0: return 10;
        default: return 20;
        }
    }
    [[nodiscard]] auto norm_len(std::uint32_t docid) const -> float {
        return doc_len(docid) / avg_len();
    }
    [[nodiscard]] auto doc_len(std::uint32_t docid) const -> std::size_t {
        switch (docid) {
        case 0: return 50;
        case 1: return 40;
        case 2: return 60;
        default: return 50;
        }
    }
    [[nodiscard]] auto term_occurrence_count(std::uint32_t term_id) const -> std::size_t {
        return 100;
    }
    [[nodiscard]] auto num_docs() const -> std::size_t { return 1000; }
    [[nodiscard]] auto avg_len() const -> float { return 50.0; }
    [[nodiscard]] auto collection_len() const -> std::size_t { return 10000; }
};

TEST_CASE("BM25", "[scorer][unit]") {
    WandData wdata;
    auto scorer = scorer::from_params(ScorerParams("bm25"), wdata);
    auto term_scorer = scorer->term_scorer(0);
    CHECK(term_scorer(0, 10) == Approx(7.92568));
    CHECK(term_scorer(0, 20) == Approx(8.26697));
    CHECK(term_scorer(1, 10) == Approx(7.97838));
    CHECK(term_scorer(1, 20) == Approx(8.29555));
}

TEST_CASE("QLD", "[scorer][unit]") {
    WandData wdata;
    auto scorer = scorer::from_params(ScorerParams("qld"), wdata);
    auto term_scorer = scorer->term_scorer(0);
    CHECK(term_scorer(0, 10) == Approx(0.64436));
    CHECK(term_scorer(0, 20) == Approx(1.04982));
    CHECK(term_scorer(1, 10) == Approx(0.65393));
    CHECK(term_scorer(1, 20) == Approx(1.05939));
}

TEST_CASE("PL2", "[scorer][unit]") {
    WandData wdata;
    auto scorer = scorer::from_params(ScorerParams("pl2"), wdata);
    auto term_scorer = scorer->term_scorer(0);
    CHECK(term_scorer(0, 10) == Approx(6.93522));
    CHECK(term_scorer(0, 20) == Approx(8.10274));
    CHECK(term_scorer(1, 10) == Approx(7.20648));
    CHECK(term_scorer(1, 20) == Approx(8.35714));
}

TEST_CASE("DPH", "[scorer][unit]") {
    WandData wdata;
    auto scorer = scorer::from_params(ScorerParams("dph"), wdata);
    auto term_scorer = scorer->term_scorer(0);
    CHECK(term_scorer(0, 10) == Approx(4.02992));
    CHECK(term_scorer(0, 20) == Approx(2.67421));
    CHECK(term_scorer(1, 10) == Approx(3.70417));
    CHECK(term_scorer(1, 20) == Approx(1.93217));
}

TEST_CASE("Quantized", "[scorer][unit]") {
    WandData wdata;
    auto scorer = scorer::from_params(ScorerParams("quantized"), wdata);
    auto term_scorer = scorer->term_scorer(0);
    CHECK(term_scorer(0, 10) == Approx(10.0F));
    CHECK(term_scorer(0, 20) == Approx(20.0F));
    CHECK(term_scorer(1, 10) == Approx(10.0F));
    CHECK(term_scorer(1, 20) == Approx(20.0F));
}
