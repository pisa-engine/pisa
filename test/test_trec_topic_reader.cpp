#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include <optional>

#include "pisa/query/trec_topic_reader.hpp"

using namespace pisa;

TEST_CASE("Read topic", "[unit]")
{
    std::istringstream is(
        "<top>\n"
        "<num> Number: 301 \n"
        "<title> title here. \n"
        "<desc> Description: \n"
        "Some description here. \n"
        "<narr> Narrative:\n"
        "Some narrative content. \n"
        "</top>\n");

    pisa::trec_topic_reader reader(is);
    auto topic = reader.next_topic();
    REQUIRE(topic);
    REQUIRE((*topic).num == "301");
    REQUIRE((*topic).title == "title here.");
    REQUIRE((*topic).desc == "Some description here.");
    REQUIRE((*topic).narr == "Some narrative content.");
    REQUIRE(reader.next_topic() == std::nullopt);
}

TEST_CASE("Read multiple topics", "[unit]")
{
    std::istringstream is(
        "<top>\n"
        "<num> Number: 301 \n"
        "<title> title here. \n"
        "<desc> Description: \n"
        "Some description here. \n"
        "<narr> Narrative:\n"
        "Some narrative content.\n"
        "Some other narrative content. \n"
        "</top>\n"
        "\n\n\n\n"
        "<top>\n"
        "<num> Number: 302 \n"
        "<title> other title. \n"
        " title continuation. \n"
        "<desc>  \n"
        "Some other description. \n"
        "<narr>\n"
        "Some other narrative\n... narrative"
        "</top>\n");

    pisa::trec_topic_reader reader(is);
    auto topic = reader.next_topic();
    REQUIRE(topic);
    REQUIRE((*topic).num == "301");
    REQUIRE((*topic).title == "title here.");
    REQUIRE((*topic).desc == "Some description here.");
    REQUIRE((*topic).narr == "Some narrative content. Some other narrative content.");
    topic = reader.next_topic();
    REQUIRE(topic);
    REQUIRE((*topic).num == "302");
    REQUIRE((*topic).title == "other title.   title continuation.");
    REQUIRE((*topic).desc == "Some other description.");
    REQUIRE((*topic).narr == "Some other narrative ... narrative");
    REQUIRE(reader.next_topic() == std::nullopt);
}

TEST_CASE("Read topic with closing tags", "[unit]")
{
    std::istringstream is(
        "<top>\n"
        "<num> Number: 301 \n"
        "<title> title here. \n"
        "</title>"
        "<desc>  \n"
        "Some description here. </desc>"
        "<narr> Narrative:\n"
        "Some narrative content. \n"
        "</narr>"
        "</top>\n");

    pisa::trec_topic_reader reader(is);
    auto topic = reader.next_topic();
    REQUIRE(topic);
    REQUIRE((*topic).num == "301");
    REQUIRE((*topic).title == "title here.");
    REQUIRE((*topic).desc == "Some description here.");
    REQUIRE((*topic).narr == "Some narrative content.");
    REQUIRE(reader.next_topic() == std::nullopt);
}

TEST_CASE("Invalid topic", "[unit]")
{
    {
        std::istringstream is(
            "<top>\n"
            "Number: 301 \n"
            "</top>\n");

        pisa::trec_topic_reader reader(is);
        REQUIRE_THROWS(reader.next_topic());
    }
    {
        std::istringstream is(
            "<top>\n"
            "<num>Number: 301 \n"
            "</top>\n");

        pisa::trec_topic_reader reader(is);
        REQUIRE_THROWS(reader.next_topic());
    }
    {
        std::istringstream is(
            "<top>\n"
            "<num>Number: 301 \n"
            "<title> title here. \n"
            "</top>\n");

        pisa::trec_topic_reader reader(is);
        REQUIRE_THROWS(reader.next_topic());
    }
    {
        std::istringstream is(
            "<top>\n"
            "<num>Number: 301 \n"
            "<title> title here. \n"
            "<desc> description here. \n"
            "</top>\n");

        pisa::trec_topic_reader reader(is);
        REQUIRE_THROWS(reader.next_topic());
    }
    {
        std::istringstream is(
            "<top>\n"
            "<num>Number: 301 \n"
            "<title> title here. \n"
            "<desc> description here. \n"
            "<narr> narrative here. \n");

        pisa::trec_topic_reader reader(is);
        REQUIRE_THROWS(reader.next_topic());
    }
}
