#define CATCH_CONFIG_MAIN

#include <span>

#include <boost/iterator/filter_iterator.hpp>
#include <boost/spirit/include/lex_lexertl.hpp>
#include <catch2/catch.hpp>

#include "payload_vector.hpp"
#include "query.hpp"
#include "query/query_parser.hpp"
#include "temporary_directory.hpp"
#include "term_map.hpp"
#include "token_filter.hpp"
#include "tokenizer.hpp"

using namespace pisa;

TEST_CASE("WhitespaceTokenizer") {
    WHEN("Empty input") {
        std::string input = "";
        WhitespaceTokenStream tok(input);
        REQUIRE(tok.next() == std::nullopt);
    }
    WHEN("Input with only whitespaces") {
        std::string input = " \t  ";
        WhitespaceTokenStream tok(input);
        REQUIRE(tok.next() == std::nullopt);
    }
    WHEN("Input without spaces around") {
        std::string input = "dog cat";
        WhitespaceTokenStream tok(input);
        REQUIRE(tok.next() == "dog");
        REQUIRE(tok.next() == "cat");
        REQUIRE(tok.next() == std::nullopt);
    }
    WHEN("Input with spaces around") {
        std::string input = "\tbling ##ing\tsting  ?*I(*&())  ";
        WhitespaceTokenStream tok(input);
        REQUIRE(tok.next() == "bling");
        REQUIRE(tok.next() == "##ing");
        REQUIRE(tok.next() == "sting");
        REQUIRE(tok.next() == "?*I(*&())");
        REQUIRE(tok.next() == std::nullopt);
    }
    SECTION("With iterators") {
        std::string input = "\tbling ##ing\tsting  ?*I(*&())  ";
        WhitespaceTokenStream tok(input);
        REQUIRE(
            std::vector<std::string>(tok.begin(), tok.end())
            == std::vector<std::string>{"bling", "##ing", "sting", "?*I(*&())"}
        );
    }
}

TEST_CASE("EnglishTokenizer") {
    SECTION("With next()") {
        std::string str("a 1 12 w0rd, token-izer. pup's, U.S.a., us., hel.lo");
        EnglishTokenStream tok(str);
        REQUIRE(tok.next() == "a");
        REQUIRE(tok.next() == "1");
        REQUIRE(tok.next() == "12");
        REQUIRE(tok.next() == "w0rd");
        REQUIRE(tok.next() == "token");
        REQUIRE(tok.next() == "izer");
        REQUIRE(tok.next() == "pup");
        REQUIRE(tok.next() == "USa");
        REQUIRE(tok.next() == "us");
        REQUIRE(tok.next() == "hel");
        REQUIRE(tok.next() == "lo");
        REQUIRE(tok.next() == std::nullopt);
    }
    SECTION("With iterators") {
        std::string str("a 1 12 w0rd, token-izer. pup's, U.S.a., us., hel.lo");
        EnglishTokenStream tokenizer(str);
        REQUIRE(
            std::vector<std::string>(tokenizer.begin(), tokenizer.end())
            == std::vector<std::string>{
                "a", "1", "12", "w0rd", "token", "izer", "pup", "USa", "us", "hel", "lo"
            }
        );
    }
}

TEST_CASE("Parse query terms to ids") {
    pisa::TemporaryDirectory tmpdir;
    auto lexfile = tmpdir.path() / "lex";
    encode_payload_vector(
        std::span<std::string const>(std::vector<std::string>{"lol", "obama", "term2", "tree", "usa"})
    )
        .to_file(lexfile.string());

    auto [query, id, parsed] = GENERATE(
        table<std::string, std::optional<std::string>, std::vector<WeightedTerm>>(
            {{"17:obama family tree", "17", {{1, 1.0}, {3, 1.0}}},
             {"obama family tree", std::nullopt, {{1, 1.0}, {3, 1.0}}},
             {"obama, family, trees", std::nullopt, {{1, 1.0}, {3, 1.0}}},
             {"obama + family + tree", std::nullopt, {{1, 1.0}, {3, 1.0}}},
             {"lol's", std::nullopt, {{0, 1.0}}},
             {"U.S.A.!?", std::nullopt, {{4, 1.0}}}}
        )
    );
    CAPTURE(query);

    auto analyzer = TextAnalyzer(std::make_unique<EnglishTokenizer>());
    analyzer.emplace_token_filter<KrovetzStemmer>();
    QueryParser parser(std::move(analyzer), std::make_unique<LexiconMap>(lexfile.string()));
    auto q = parser.parse(query);
    REQUIRE(q.id() == id);
    REQUIRE(q.terms() == parsed);
}
