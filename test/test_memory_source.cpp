#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <fstream>

#include "pisa/io.hpp"
#include "pisa/memory_source.hpp"
#include "temporary_directory.hpp"

using pisa::MemorySource;
using pisa::io::NoSuchFile;

TEST_CASE("Empty memory source", "[mmap][io]")
{
    MemorySource source;
    REQUIRE_FALSE(source.is_mapped());
    REQUIRE(source.size() == 0);
    REQUIRE_THROWS_AS(source.begin(), std::domain_error);
    REQUIRE_THROWS_AS(source.end(), std::domain_error);
    REQUIRE(source.span().empty());
    REQUIRE(source.subspan(0).empty());
    REQUIRE_THROWS_AS(source.subspan(1), std::out_of_range);
}

TEST_CASE("Error when mapping non-existent file", "[mmap][io]")
{
    pisa::TemporaryDirectory temp;
    auto file_path = (temp.path() / "file");
    REQUIRE_THROWS_AS(MemorySource::mapped_file(file_path), NoSuchFile);
}

TEST_CASE("Non-empty memory source", "[mmap][io]")
{
    pisa::TemporaryDirectory temp;
    auto file_path = (temp.path() / "file");
    {
        std::ofstream os(file_path.string());
        os << "Lorem ipsum";
    }
    auto source = MemorySource::mapped_file(file_path);
    REQUIRE(source.is_mapped());
    REQUIRE(source.size() == 11);
    REQUIRE(std::string(source.begin(), source.end()) == "Lorem ipsum");
    REQUIRE(std::string_view(source.data(), source.size()) == "Lorem ipsum");
    auto bytes = source.span();
    REQUIRE(std::string(bytes.begin(), bytes.end()) == "Lorem ipsum");
    bytes = source.subspan(1, 4);
    REQUIRE(std::string(bytes.begin(), bytes.end()) == "orem");
    REQUIRE(source.subspan(11).empty());
    REQUIRE_THROWS_AS(source.subspan(12), std::out_of_range);
    REQUIRE_THROWS_AS(source.subspan(1, source.size()), std::out_of_range);
}
