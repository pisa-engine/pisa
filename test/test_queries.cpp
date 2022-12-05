#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "query/algorithm.hpp"
#include "temporary_directory.hpp"

using namespace pisa;

TEST_CASE("Parse query term ids without query id")
{
    auto raw_query = "1 2\t3    4";
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id.has_value() == false);
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}

TEST_CASE("Parse query term ids with query id")
{
    auto raw_query = "1: 1\t2 3\t4";
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id == "1");
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}

TEST_CASE("Compute parsing function")
{
    pisa::TemporaryDirectory tmpdir;

    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"a", "account", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());
    auto stopwords_filename = tmpdir.path() / "stop";
    {
        std::ofstream os(stopwords_filename.string());
        os << "a\nthe\n";
    }

    std::vector<Query> queries;

    WHEN("No stopwords, terms, or stemmer")
    {
        // Note we don't need a tokenizer because ID parsing does not use it
        auto parse = resolve_query_parser(queries, nullptr, std::nullopt, std::nullopt, std::nullopt);
        THEN("Parse query IDs")
        {
            parse("1:0 2 4");
            REQUIRE(queries[0].id == std::optional<std::string>("1"));
            REQUIRE(queries[0].terms == std::vector<term_id_type>{0, 2, 4});
            REQUIRE(queries[0].term_weights.empty());
        }
    }
    WHEN("With terms and stopwords. No stemmer")
    {
        auto parse = resolve_query_parser(
            queries,
            std::make_unique<WhitespaceTokenizer>(),
            lexfile.string(),
            stopwords_filename.string(),
            std::nullopt);
        THEN("Parse query IDs")
        {
            parse("1:a he usa");
            REQUIRE(queries[0].id == std::optional<std::string>("1"));
            REQUIRE(queries[0].terms == std::vector<term_id_type>{2, 4});
            REQUIRE(queries[0].term_weights.empty());
        }
    }
    WHEN("With terms, stopwords, and stemmer")
    {
        auto parse = resolve_query_parser(
            queries,
            std::make_unique<WhitespaceTokenizer>(),
            lexfile.string(),
            stopwords_filename.string(),
            "porter2");
        THEN("Parse query IDs")
        {
            parse("1:a he usa");
            REQUIRE(queries[0].id == std::optional<std::string>("1"));
            REQUIRE(queries[0].terms == std::vector<term_id_type>{2, 4});
            REQUIRE(queries[0].term_weights.empty());
        }
    }
    WHEN("Parser with whitespace tokenizer")
    {
        auto parse = resolve_query_parser(
            queries,
            std::make_unique<WhitespaceTokenizer>(),
            lexfile.string(),
            std::nullopt,
            std::nullopt);
        THEN("Parses usa's as usa's (and does not find it in lexicon)")
        {
            parse("1:a he usa's");
            REQUIRE(queries[0].terms == std::vector<term_id_type>{0, 2});
        }
    }
    WHEN("Parser with English tokenizer")
    {
        auto parse = resolve_query_parser(
            queries, std::make_unique<EnglishTokenizer>(), lexfile.string(), std::nullopt, std::nullopt);
        THEN("Parses usa's as usa (and finds it in lexicon)")
        {
            parse("1:a he usa's");
            REQUIRE(queries[0].terms == std::vector<term_id_type>{0, 2, 4});
        }
    }
}

TEST_CASE("Load stopwords in term processor with all stopwords present in the lexicon")
{
    pisa::TemporaryDirectory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"a", "account", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "a\nshe\nhe";
    is.close();

    TermProcessor tprocessor(
        std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(tprocessor.get_stopwords() == std::vector<std::uint32_t>{0, 2, 3});
}

TEST_CASE("Load stopwords in term processor with some stopwords not present in the lexicon")
{
    pisa::TemporaryDirectory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"account", "coffee", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "\nis\nto\na\nshe\nhe";
    is.close();

    TermProcessor tprocessor(
        std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(tprocessor.get_stopwords() == std::vector<std::uint32_t>{2, 3});
}

TEST_CASE("Check if term is stopword")
{
    pisa::TemporaryDirectory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"account", "coffee", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "\nis\nto\na\nshe\nhe";
    is.close();

    TermProcessor tprocessor(
        std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(!tprocessor.is_stopword(0));
    REQUIRE(!tprocessor.is_stopword(1));
    REQUIRE(tprocessor.is_stopword(2));
    REQUIRE(tprocessor.is_stopword(3));
    REQUIRE(!tprocessor.is_stopword(4));
    REQUIRE(!tprocessor.is_stopword(5));
}
