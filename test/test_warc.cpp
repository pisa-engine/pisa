#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <string>

#include "parsing/warc.hpp"

using namespace ds2i;

TEST_CASE("Parse WARC version", "[warc][unit]")
{
    std::string input = GENERATE(as<std::string>(),
                                 "WARC/0.18",
                                 "WARC/0.18\nUnrelated text",
                                 "\n\n\nWARC/0.18\nUnrelated text");
    GIVEN("Input: " << input) {
        std::istringstream in("WARC/0.18\nUnrelated text");
        std::string        version;
        warc::read_version(in, version);
        REQUIRE(version == "0.18");
    }
}

TEST_CASE("Parse invalid WARC version string", "[warc][unit]")
{
    std::istringstream in("INVALID_STRING");
    std::string version;
    REQUIRE_THROWS_AS(warc::read_version(in, version), Warc_Format_Error);
}

TEST_CASE("Look for version until EOF", "[warc][unit]")
{
    std::istringstream in("\n");
    std::string version = "initial";
    REQUIRE(warc::read_version(in, version).eof());
    REQUIRE(version == "initial");
}

TEST_CASE("Parse valid fields", "[warc][unit]")
{
    std::string input = GENERATE(as<std::string>(),
                                 "WARC-Type: warcinfo\n"
                                 "Content-Type  : application/warc-fields\n"
                                 "Content-Length: 219    \n"
                                 "\n"
                                 "REMAINDER",
                                 "WARC-Type: warcinfo\n"
                                 "Content-Type  : application/warc-fields\n"
                                 "Content-Length: 219    \r\n"
                                 "\r\n"
                                 "REMAINDER");
    GIVEN("A valid list of fields") {
        std::istringstream in(input);
        WHEN("Parse fields") {
            Field_Map fields;
            warc::read_fields(in, fields);
            THEN("Read fields are lowercase and stripped") {
                CHECK(fields["warc-type"] == "warcinfo");
                CHECK(fields["content-type"] == "application/warc-fields");
                CHECK(fields["content-length"] == "219");
            }
            THEN("Two trailing newlines are skipped") {
                std::ostringstream os;
                os << in.rdbuf();
                REQUIRE(os.str() == "REMAINDER");
            }
        }
    }
}

std::string warcinfo() {
    return "WARC/0.18\n"
           "WARC-Type: warcinfo\n"
           "WARC-Date: 2009-03-65T08:43:19-0800\n"
           "WARC-Record-ID: <urn:uuid:993d3969-9643-4934-b1c6-68d4dbe55b83>\n"
           "Content-Type: application/warc-fields\n"
           "Content-Length: 219\n"
           "\n"
           "software: Nutch 1.0-dev (modified for clueweb09)\n"
           "isPartOf: clueweb09-en\n"
           "description: clueweb09 crawl with WARC output\n"
           "format: WARC file version 0.18\n"
           "conformsTo: "
           "http://www.archive.org/documents/WarcFileFormat-0.18.html\n"
           "\n";
}

TEST_CASE("Parse warcinfo record", "[warc][unit]")
{
    std::istringstream in(warcinfo());
    Warc_Record record;
    read_warc_record(in, record);
    CHECK(in.peek() == EOF);
    CHECK(*record.http_field("conformsto") ==
          "http://www.archive.org/documents/WarcFileFormat-0.18.html");
}

std::string response() {
    return "WARC/0.18\n"
           "WARC-Type: response\n"
           "WARC-Target-URI: http://00000-nrt-realestate.homepagestartup.com/\n"
           "WARC-Warcinfo-ID: 993d3969-9643-4934-b1c6-68d4dbe55b83\n"
           "WARC-Date: 2009-03-65T08:43:19-0800\n"
           "WARC-Record-ID: <urn:uuid:67f7cabd-146c-41cf-bd01-04f5fa7d5229>\n"
           "WARC-TREC-ID: clueweb09-en0000-00-00000\n"
           "Content-Type: application/http;msgtype=response\n"
           "WARC-Identified-Payload-Type: \n"
           "Content-Length: 16558\n"
           "\n"
           "HTTP/1.1 200 OK\n"
           "Content-Type: text/html\n"
           "Date: Tue, 13 Jan 2009 18:05:10 GMT\n"
           "Pragma: no-cache\n"
           "Cache-Control: no-cache, must-revalidate\n"
           "X-Powered-By: PHP/4.4.8\n"
           "Server: WebServerX\n"
           "Connection: close\n"
           "Last-Modified: Tue, 13 Jan 2009 18:05:10 GMT\n"
           "Expires: Mon, 20 Dec 1998 01:00:00 GMT\n"
           "Content-Length: 10\n"
           "\n"
           "Content...";
}

TEST_CASE("Parse response record", "[warc][unit]")
{
    std::istringstream in(response());
    Warc_Record record;
    read_warc_record(in, record);
    CHECK(in.peek() == EOF);
    CHECK(record.type() == "response");
    CHECK(record.content() == "Content...");
}
