#define CATCH_CONFIG_MAIN

#include <optional>

#include <catch2/catch.hpp>

#include "pisa/token_filter.hpp"

using namespace pisa;

TEST_CASE("Lowercase filter")
{
    LowercaseFilter lowercase;
    auto stream = lowercase.filter(std::string_view("WoRd"));
    REQUIRE(stream->next() == "word");
    REQUIRE(stream->next() == std::nullopt);
}

TEST_CASE("Stop word remover")
{
    std::unordered_set<std::string> stopwords;
    stopwords.insert("the");
    stopwords.insert("a");
    StopWordRemover remover(std::move(stopwords));

    REQUIRE(remover.filter(std::string_view("the"))->collect().empty());
    REQUIRE(remover.filter(std::string_view("a"))->collect().empty());
    REQUIRE(remover.filter(std::string_view("word"))->collect() == std::vector<std::string>{"word"});
}

TEST_CASE("Porter2")
{
    Porter2Stemmer stemmer;
    SECTION("word")
    {
        auto stream = stemmer.filter(std::string_view("word"));
        REQUIRE(stream->next() == "word");
        REQUIRE(stream->next() == std::nullopt);
    }
    SECTION("playing")
    {
        auto stream = stemmer.filter(std::string_view("playing"));
        REQUIRE(stream->next() == "play");
        REQUIRE(stream->next() == std::nullopt);
    }
    SECTION("I")
    {
        auto stream = stemmer.filter(std::string_view("I"));
        REQUIRE(stream->next() == "I");
        REQUIRE(stream->next() == std::nullopt);
    }
    SECTION("flying")
    {
        auto stream = stemmer.filter(std::string_view("flying"));
        REQUIRE(stream->next() == "fli");
        REQUIRE(stream->next() == std::nullopt);
    }
}

TEST_CASE("Krovetz")
{
    KrovetzStemmer stemmer;
    SECTION("word")
    {
        auto stream = stemmer.filter(std::string_view("word"));
        REQUIRE(stream->next() == "word");
        REQUIRE(stream->next() == std::nullopt);
    }
    SECTION("playing")
    {
        auto stream = stemmer.filter(std::string_view("playing"));
        REQUIRE(stream->next() == "play");
        REQUIRE(stream->next() == std::nullopt);
    }
    // Notice the difference between Porter2 and Krovetz in the following two tests
    SECTION("I")
    {
        auto stream = stemmer.filter(std::string_view("I"));
        REQUIRE(stream->next() == "i");
        REQUIRE(stream->next() == std::nullopt);
    }
    SECTION("flying")
    {
        auto stream = stemmer.filter(std::string_view("flying"));
        REQUIRE(stream->next() == "flying");
        REQUIRE(stream->next() == std::nullopt);
    }
}
