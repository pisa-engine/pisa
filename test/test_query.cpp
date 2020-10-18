#define CATCH_CONFIG_MAIN

#include <sstream>

#include <catch2/catch.hpp>
#include <gsl/span>

#include "query.hpp"

using pisa::QueryContainer;
using pisa::RequestFlag;
using pisa::RequestFlagSet;
using pisa::TermId;
using pisa::TermPair;

TEST_CASE("Construct from raw string")
{
    auto raw_query = "brooklyn tea house";
    auto query = QueryContainer::raw(raw_query);
    REQUIRE(*query.string() == raw_query);
}

TEST_CASE("Construct from terms")
{
    std::vector<std::string> terms{"brooklyn", "tea", "house"};
    auto query = QueryContainer::from_terms(terms, std::nullopt);
    REQUIRE(*query.terms() == std::vector<std::string>{"brooklyn", "tea", "house"});
}

TEST_CASE("Construct from terms with processor")
{
    std::vector<std::string> terms{"brooklyn", "tea", "house"};
    auto proc = [](std::string term) -> std::optional<std::string> {
        if (term.size() > 3) {
            return term.substr(0, 4);
        }
        return std::nullopt;
    };
    auto query = QueryContainer::from_terms(terms, proc);
    REQUIRE(*query.terms() == std::vector<std::string>{"broo", "hous"});
}

TEST_CASE("Construct from term IDs")
{
    std::vector<std::uint32_t> term_ids{1, 0, 3};
    auto query = QueryContainer::from_term_ids(term_ids);
    REQUIRE(*query.term_ids() == std::vector<std::uint32_t>{1, 0, 3});
}

TEST_CASE("Parse query")
{
    auto raw_query = "brooklyn tea house brooklyn";
    auto query = QueryContainer::raw(raw_query);
    std::vector<std::string> lexicon{"house", "brooklyn"};
    auto term_proc = [](std::string term) -> std::optional<std::string> { return term; };
    query.parse([&](auto&& q) {
        std::istringstream is(q);
        std::string term;
        std::vector<pisa::ResolvedTerm> parsed_terms;
        while (is >> term) {
            if (auto t = term_proc(term); t) {
                if (auto pos = std::find(lexicon.begin(), lexicon.end(), *t); pos != lexicon.end()) {
                    auto id = static_cast<std::uint32_t>(std::distance(lexicon.begin(), pos));
                    parsed_terms.push_back(pisa::ResolvedTerm{id, *t});
                }
            }
        }
        return parsed_terms;
    });
    REQUIRE(*query.term_ids() == std::vector<std::uint32_t>{1, 0, 1});
}

TEST_CASE("Parsing throws without raw query")
{
    std::vector<std::uint32_t> term_ids{1, 0, 3};
    auto query = QueryContainer::from_term_ids(term_ids);
    REQUIRE_THROWS_AS(
        query.parse([](auto&& str) { return std::vector<pisa::ResolvedTerm>{}; }), std::domain_error);
}

TEST_CASE("Parse query container from colon-delimited format")
{
    auto query = QueryContainer::from_colon_format("");
    REQUIRE(query.string()->empty());
    REQUIRE_FALSE(query.id());

    query = QueryContainer::from_colon_format("brooklyn tea house");
    REQUIRE(*query.string() == "brooklyn tea house");
    REQUIRE_FALSE(query.id());

    query = QueryContainer::from_colon_format("BTH:brooklyn tea house");
    REQUIRE(*query.string() == "brooklyn tea house");
    REQUIRE(*query.id() == "BTH");

    query = QueryContainer::from_colon_format("BTH:");
    REQUIRE(query.string()->empty());
    REQUIRE(*query.id() == "BTH");
}

TEST_CASE("Parse query container from JSON")
{
    REQUIRE_THROWS_AS(QueryContainer::from_json(""), std::runtime_error);
    REQUIRE_THROWS_AS(QueryContainer::from_json(R"({"id":"ID"})"), std::invalid_argument);

    auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "selections": [{"k": 10, "intersections": [1, 2, 3, 4]}]
    }
    )");
    REQUIRE(*query.id() == "ID");
    REQUIRE(*query.string() == "brooklyn tea house");
    REQUIRE_FALSE(query.terms());
    REQUIRE_FALSE(query.term_ids());
    REQUIRE(query.thresholds().empty());
    auto selection = query.selection(10);
    REQUIRE(selection.has_value());
    REQUIRE(selection->selected_terms == std::vector<std::size_t>{0, 1, 2});
    REQUIRE(selection->selected_pairs == std::vector<std::array<std::size_t, 2>>{{0, 1}});

    query = QueryContainer::from_json(R"(
    {
        "term_ids": [1, 0, 3],
        "terms": ["brooklyn", "tea", "house"],
        "thresholds": [{"k": 10, "score": 10.8}]
    }
    )");
    REQUIRE(*query.terms() == std::vector<std::string>{"brooklyn", "tea", "house"});
    REQUIRE(*query.term_ids() == std::vector<std::uint32_t>{1, 0, 3});
    REQUIRE(*query.threshold(10) == Approx(10.8));
    REQUIRE_FALSE(query.id());
    REQUIRE_FALSE(query.string());

    REQUIRE_THROWS_AS(QueryContainer::from_json(R"({"terms":[1, 2]})"), std::runtime_error);
}

TEST_CASE("Serialize query container to JSON")
{
    auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "terms": ["brooklyn", "tea", "house"],
        "term_ids": [1, 0, 3],
        "thresholds": [{"k": 10, "score": 10.0}],
        "selections": [{"k": 10, "intersections": [1, 2, 4, 5]}]
    }
    )");
    auto serialized = query.to_json_string();
    REQUIRE(
        serialized
        == R"({"id":"ID","query":"brooklyn tea house","selections":[{"intersections":[1,2,4,5],"k":10}],"term_ids":[1,0,3],"terms":["brooklyn","tea","house"],"thresholds":[{"k":10,"score":10.0}]})");
}

TEST_CASE("Copy constructor and assignment")
{
    auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "terms": ["brooklyn", "tea", "house"],
        "term_ids": [1, 0, 3],
        "thresholds": [{"k": 10, "score": 10.0}],
        "selections": [{"k": 10, "intersections": [1, 2, 4]}]
    }
    )");
    {
        QueryContainer copy(query);
        REQUIRE(query.string() == copy.string());
        REQUIRE(*query.id() == copy.id());
        REQUIRE(*query.terms() == *copy.terms());
        REQUIRE(*query.term_ids() == *copy.term_ids());
        REQUIRE(query.thresholds() == copy.thresholds());
        REQUIRE(query.selections() == copy.selections());
    }
    {
        auto copy = QueryContainer::raw("");
        copy = query;
        REQUIRE(query.string() == copy.string());
        REQUIRE(*query.id() == copy.id());
        REQUIRE(*query.terms() == *copy.terms());
        REQUIRE(*query.term_ids() == *copy.term_ids());
        REQUIRE(query.thresholds() == copy.thresholds());
        REQUIRE(query.selections() == copy.selections());
    }
}

TEST_CASE("Filter terms")
{
    SECTION("Both terms and IDs")
    {
        auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "terms": ["brooklyn", "tea", "house"],
        "term_ids": [1, 0, 3],
        "thresholds": [{"k": 10, "score": 10.0}]
    }
    )");
        SECTION("First")
        {
            query.filter_terms(std::vector<std::size_t>{0});
            REQUIRE(*query.terms() == std::vector<std::string>{"brooklyn"});
            REQUIRE(*query.term_ids() == std::vector<TermId>{1});
        }
        SECTION("Second")
        {
            query.filter_terms(std::vector<std::size_t>{1});
            REQUIRE(*query.terms() == std::vector<std::string>{"tea"});
            REQUIRE(*query.term_ids() == std::vector<TermId>{0});
        }
        SECTION("Third")
        {
            query.filter_terms(std::vector<std::size_t>{2});
            REQUIRE(*query.terms() == std::vector<std::string>{"house"});
            REQUIRE(*query.term_ids() == std::vector<TermId>{3});
        }
    }
    SECTION("Only terms")
    {
        auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "terms": ["brooklyn", "tea", "house"],
        "thresholds": [{"k": 10, "score": 10.0}]
    }
    )");
        query.filter_terms(std::vector<std::size_t>{1});
        REQUIRE(*query.terms() == std::vector<std::string>{"tea"});
    }
    SECTION("Only IDs")
    {
        auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "term_ids": [1, 0, 3],
        "thresholds": [{"k": 10, "score": 10.0}]
    }
    )");
        query.filter_terms(std::vector<std::size_t>{1});
        REQUIRE(*query.term_ids() == std::vector<TermId>{0});
    }
}

TEST_CASE("Request flags")
{
    auto flags = RequestFlagSet::all();
    REQUIRE(flags.contains(RequestFlag::Threshold));
    REQUIRE(flags.contains(RequestFlag::Weights));
    flags.remove(RequestFlag::Threshold);
    REQUIRE(not flags.contains(RequestFlag::Threshold));
    REQUIRE(flags.contains(RequestFlag::Weights));
    REQUIRE(not(RequestFlagSet::all() ^ RequestFlag::Threshold).contains(RequestFlag::Threshold));
    REQUIRE((RequestFlagSet::all() ^ RequestFlag::Threshold).contains(RequestFlag::Selection));
    REQUIRE((RequestFlagSet::all() ^ RequestFlag::Threshold).contains(RequestFlag::Weights));
}

TEST_CASE("QueryRequest")
{
    auto query = QueryContainer::from_json(R"(
    {
        "id": "ID",
        "query": "brooklyn tea house",
        "terms": ["brooklyn", "tea", "house"],
        "term_ids": [1, 0, 3],
        "thresholds": [{"k": 10, "score": 10.0}],
        "selections": [{"k": 10, "intersections": [1, 2, 4, 5]}]
    }
    )");
    SECTION("all")
    {
        auto request = query.query(10);
        REQUIRE(request.k() == 10);
        REQUIRE(request.term_ids() == gsl::make_span(std::vector<TermId>{0, 1, 3}));
        REQUIRE(request.term_weights() == gsl::make_span(std::vector<float>{1.0, 1.0, 1.0}));
        REQUIRE(request.threshold().has_value());
        REQUIRE(*request.threshold() == 10.0);
        REQUIRE(request.selection().has_value());
        auto selection = *request.selection();
        REQUIRE(selection.selected_pairs == std::vector<TermPair>{{1, 3}});
        REQUIRE(selection.selected_terms == std::vector<TermId>{0, 1, 3});
    }
    SECTION("different k")
    {
        auto request = query.query(5);
        REQUIRE(request.k() == 5);
        REQUIRE(request.term_ids() == gsl::make_span(std::vector<TermId>{0, 1, 3}));
        REQUIRE(request.term_weights() == gsl::make_span(std::vector<float>{1.0, 1.0, 1.0}));
        REQUIRE(not request.threshold().has_value());
        REQUIRE(not request.selection().has_value());
    }
    SECTION("Suppress threshold")
    {
        auto request = query.query(10, RequestFlagSet::all() ^ RequestFlag::Threshold);
        REQUIRE(request.k() == 10);
        REQUIRE(request.term_ids() == gsl::make_span(std::vector<TermId>{0, 1, 3}));
        REQUIRE(request.term_weights() == gsl::make_span(std::vector<float>{1.0, 1.0, 1.0}));
        REQUIRE(not request.threshold().has_value());
        REQUIRE(request.selection().has_value());
        auto selection = *request.selection();
        REQUIRE(selection.selected_pairs == std::vector<TermPair>{{1, 3}});
        REQUIRE(selection.selected_terms == std::vector<TermId>{0, 1, 3});
    }
    SECTION("Suppress selection")
    {
        auto request = query.query(10, RequestFlagSet::all() ^ RequestFlag::Selection);
        REQUIRE(request.k() == 10);
        REQUIRE(request.term_ids() == gsl::make_span(std::vector<TermId>{0, 1, 3}));
        REQUIRE(request.term_weights() == gsl::make_span(std::vector<float>{1.0, 1.0, 1.0}));
        REQUIRE(request.threshold().has_value());
        REQUIRE(*request.threshold() == 10.0);
        REQUIRE(not request.selection().has_value());
    }
}

TEST_CASE("TermPair")
{
    SECTION("Constructor")
    {
        REQUIRE(TermPair(0, 1) == TermPair(1, 0));
        REQUIRE(TermPair(std::array<TermId, 2>{1, 0}) == TermPair(0, 1));
    }
    SECTION("From array")
    {
        TermPair tp(0, 1);
        tp = std::array<TermId, 2>{1, 0};
        REQUIRE(tp == TermPair(0, 1));
    }
    SECTION("Accessors")
    {
        TermPair tp(1, 0);
        REQUIRE(tp.at(0) == 0);
        REQUIRE(tp.at(1) == 1);
        REQUIRE_THROWS_AS(tp.at(2), std::out_of_range);
        REQUIRE(std::get<0>(tp) == 0);
        REQUIRE(std::get<1>(tp) == 1);
        REQUIRE(tp.front() == 0);
        REQUIRE(tp.back() == 1);
        REQUIRE(*tp.data() == 0);
        REQUIRE(*std::next(tp.data()) == 1);
        REQUIRE(std::vector<TermId>(tp.begin(), tp.end()) == std::vector<TermId>{0, 1});
        REQUIRE(std::vector<TermId>(tp.cbegin(), tp.cend()) == std::vector<TermId>{0, 1});
    }
    SECTION("swap")
    {
        TermPair tp1(1, 0);
        TermPair tp2(4, 5);
        std::swap(tp1, tp2);
        REQUIRE(tp1 == TermPair(4, 5));
        REQUIRE(tp2 == TermPair(0, 1));
    }
}
