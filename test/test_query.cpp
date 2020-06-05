#define CATCH_CONFIG_MAIN

#include <sstream>

#include <catch2/catch.hpp>

#include "query.hpp"

using pisa::QueryContainer;
using pisa::TermId;

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
        "query": "brooklyn tea house"
    }
    )");
    REQUIRE(*query.id() == "ID");
    REQUIRE(*query.string() == "brooklyn tea house");
    REQUIRE_FALSE(query.terms());
    REQUIRE_FALSE(query.term_ids());
    REQUIRE(query.thresholds().empty());

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
        "thresholds": [{"k": 10, "score": 10.0}]
    }
    )");
    auto serialized = query.to_json_string();
    REQUIRE(
        serialized
        == R"({"id":"ID","query":"brooklyn tea house","term_ids":[1,0,3],"terms":["brooklyn","tea","house"],"thresholds":[{"k":10,"score":10.0}]})");
}

TEST_CASE("Copy constructor and assignment")
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
    {
        QueryContainer copy(query);
        REQUIRE(query.string() == copy.string());
        REQUIRE(*query.id() == copy.id());
        REQUIRE(*query.terms() == *copy.terms());
        REQUIRE(*query.term_ids() == *copy.term_ids());
        REQUIRE(query.thresholds() == copy.thresholds());
    }
    {
        auto copy = QueryContainer::raw("");
        copy = query;
        REQUIRE(query.string() == copy.string());
        REQUIRE(*query.id() == copy.id());
        REQUIRE(*query.terms() == *copy.terms());
        REQUIRE(*query.term_ids() == *copy.term_ids());
        REQUIRE(query.thresholds() == copy.thresholds());
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
