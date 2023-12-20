#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "pisa/text_analyzer.hpp"

using namespace pisa;

TEST_CASE("No token filters") {
    TextAnalyzer analyzer(std::make_unique<WhitespaceTokenizer>());
    REQUIRE(
        analyzer.analyze("Lorem ipsum dolor sit amet")->collect()
        == std::vector<std::string>{"Lorem", "ipsum", "dolor", "sit", "amet"}
    );
}

TEST_CASE("One filter") {
    std::unordered_set<std::string> stopwords{"sit"};
    TextAnalyzer analyzer(std::make_unique<WhitespaceTokenizer>());
    analyzer.emplace_token_filter<LowercaseFilter>();
    REQUIRE(
        analyzer.analyze("Lorem ipsum dolor sit amet")->collect()
        == std::vector<std::string>{"lorem", "ipsum", "dolor", "sit", "amet"}
    );
}

TEST_CASE("Multiple filters") {
    std::unordered_set<std::string> stopwords{"sit", "and", "the"};
    TextAnalyzer analyzer(std::make_unique<WhitespaceTokenizer>());
    analyzer.emplace_token_filter<LowercaseFilter>();
    analyzer.emplace_token_filter<StopWordRemover>(std::move(stopwords));
    analyzer.emplace_token_filter<Porter2Stemmer>();
    REQUIRE(
        analyzer.analyze("Lorem ipsum dolor sit amet and going the")->collect()
        == std::vector<std::string>{"lorem", "ipsum", "dolor", "amet", "go"}
    );
}

TEST_CASE("Removing first and last token") {
    std::unordered_set<std::string> stopwords{"lorem", "amet"};
    TextAnalyzer analyzer(std::make_unique<WhitespaceTokenizer>());
    analyzer.emplace_token_filter<LowercaseFilter>();
    analyzer.emplace_token_filter<StopWordRemover>(std::move(stopwords));
    REQUIRE(
        analyzer.analyze("Lorem ipsum dolor sit amet")->collect()
        == std::vector<std::string>{"ipsum", "dolor", "sit"}
    );
}

TEST_CASE("Multiple token filters + html filter") {
    std::unordered_set<std::string> stopwords{"sit", "and", "the"};
    TextAnalyzer analyzer(std::make_unique<WhitespaceTokenizer>());
    analyzer.emplace_token_filter<LowercaseFilter>();
    analyzer.emplace_token_filter<StopWordRemover>(std::move(stopwords));
    analyzer.emplace_token_filter<Porter2Stemmer>();
    analyzer.emplace_text_filter<StripHtmlFilter>();
    REQUIRE(
        analyzer.analyze("<p>Lorem ipsum dolor sit <emph>amet</emph> and going the</p>")->collect()
        == std::vector<std::string>{"lorem", "ipsum", "dolor", "amet", "go"}
    );
}
