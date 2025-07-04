#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "pisa/parser.hpp"

TEST_CASE("parse_jsonl_record", "[parser][unit]") {
    SECTION("empty stream") {
        std::istringstream in("");
        REQUIRE_FALSE(pisa::parse_jsonl_record(in).has_value());
    }
    SECTION("single line") {
        std::istringstream in(R"({"title":"DOC1","url":"http://localhost","content":"lorem ipsum"})");
        auto record = pisa::parse_jsonl_record(in);
        REQUIRE(record->title() == "DOC1");
        REQUIRE(record->url() == "http://localhost");
        REQUIRE(record->content() == "lorem ipsum");
        record = pisa::parse_jsonl_record(in);
        REQUIRE_FALSE(record.has_value());
    }
    SECTION("single line with empty line") {
        std::istringstream in(R"({"title":"DOC1","url":"http://localhost","content":"lorem ipsum"}
)");
        auto record = pisa::parse_jsonl_record(in);
        REQUIRE(record.has_value());
        record = pisa::parse_jsonl_record(in);
        REQUIRE_FALSE(record.has_value());
    }
    SECTION("multiple lines") {
        std::istringstream in(R"({"title":"DOC1","url":"http://localhost","content":"lorem ipsum"}
{"title":"DOC2","content":"hello world"}
{"title":"DOC3","url":"https://github.com/pisa-engine/pisa/","content":"pisa content"}
)");
        auto record = pisa::parse_jsonl_record(in);
        REQUIRE(record->title() == "DOC1");
        REQUIRE(record->url() == "http://localhost");
        REQUIRE(record->content() == "lorem ipsum");
        record = pisa::parse_jsonl_record(in);
        REQUIRE(record->title() == "DOC2");
        REQUIRE(record->url().empty());
        REQUIRE(record->content() == "hello world");
        record = pisa::parse_jsonl_record(in);
        REQUIRE(record->title() == "DOC3");
        REQUIRE(record->url() == "https://github.com/pisa-engine/pisa/");
        REQUIRE(record->content() == "pisa content");
        record = pisa::parse_jsonl_record(in);
        REQUIRE_FALSE(record.has_value());
    }
}
