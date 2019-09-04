#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include "query/queries.hpp"
#include "temporary_directory.hpp"

using namespace pisa;

TEST_CASE("Parse query term ids without query id") {
    auto raw_query = "1\t2\t3\t4";
    std::optional<std::string> id = std::nullopt;
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id.has_value() == false);
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}

TEST_CASE("Parse query term ids with query id") {
    auto raw_query = "1: 1\t2\t3\t4";
    std::optional<std::string> id = std::nullopt;
    auto q = parse_query_ids(raw_query);
    REQUIRE(q.id == "1");
    REQUIRE(q.terms == std::vector<std::uint32_t>{1, 2, 3, 4});
}

TEST_CASE("Load stopwords in term processor with all stopwords present in the lexicon") {
    Temporary_Directory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"a", "account", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "a\nshe\nhe";
    is.close();

    TermProcessor tprocessor(std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(tprocessor.get_stopwords() == std::vector<std::uint32_t>{0, 2, 3});
}

TEST_CASE("Load stopwords in term processor with some stopwords not present in the lexicon") {
    Temporary_Directory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"account", "coffee", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "\nis\nto\na\nshe\nhe";
    is.close();

    TermProcessor tprocessor(std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(tprocessor.get_stopwords() == std::vector<std::uint32_t>{2, 3});
}

TEST_CASE("Check if term is stopword")
{
    Temporary_Directory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        gsl::make_span(std::vector<std::string>{"account", "coffee", "he", "she", "usa", "world"}))
        .to_file(lexfile.string());

    auto stopwords_filename = (tmpdir.path() / "stopwords").string();
    std::ofstream is(stopwords_filename);
    is << "\nis\nto\na\nshe\nhe";
    is.close();

    TermProcessor tprocessor(std::make_optional(lexfile.string()), std::make_optional(stopwords_filename), std::nullopt);
    REQUIRE(!tprocessor.is_stopword(0));
    REQUIRE(!tprocessor.is_stopword(1));
    REQUIRE(tprocessor.is_stopword(2));
    REQUIRE(tprocessor.is_stopword(3));
    REQUIRE(!tprocessor.is_stopword(4));
    REQUIRE(!tprocessor.is_stopword(5));
}