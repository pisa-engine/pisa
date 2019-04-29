#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <algorithm>
#include <string>
#include <variant>
#include <vector>

#include "pisa/program/invert.hpp"

using namespace pisa;

[[nodiscard]] auto split(std::string str) -> std::vector<std::string>
{
    std::vector<std::string> args;
    std::istringstream is(std::move(str));
    std::string arg;
    while (is >> arg) {
        args.push_back(std::move(arg));
    }
    return args;
}

[[nodiscard]] auto transform(std::vector<std::string> const &args) -> std::vector<char const *>
{
    std::vector<char const *> argv(args.size());
    std::transform(args.begin(), args.end(), argv.begin(), [](auto const &s) { return s.c_str(); });
    return argv;
}

TEST_CASE("Correct parse", "[invert][program]")
{
    auto [args, expected] = GENERATE(table<std::string, InvertSettings>({
        {
            "invert -i input -o output --term-count 10",
            {"input", "output", std::thread::hardware_concurrency(), 10, 100'000}
        },
        {
            "invert -i input -o output --term-count 10 -j 5",
            {"input", "output", 5, 10, 100'000}
        },
        {
            "invert -i input -o output --term-count 10 --threads 32 --batch-size 127",
            {"input", "output", 32, 10, 127}
        }
    }));
    CAPTURE(args);
    auto argvec = split(args);
    auto argv = transform(argvec);
    auto result = InvertSettings::parse(argvec.size(), &argv[0]);
    auto settings = std::get_if<InvertSettings>(&result);
    REQUIRE(settings != nullptr);
    REQUIRE(settings->input_basename == expected.input_basename);
    REQUIRE(settings->output_basename == expected.output_basename);
    REQUIRE(settings->threads == expected.threads);
    REQUIRE(settings->term_count == expected.term_count);
    REQUIRE(settings->batch_size == expected.batch_size);
}

TEST_CASE("Failed parse", "[invert][program]")
{
    auto [args, exit_code] = GENERATE(table<std::string, int>(
        {{"invert -h", 0},
         {"invert -i x -o y -h", 0},
         {"invert -i x -o y", (int)CLI::ExitCodes::RequiredError},
         {"invert -i x --term-count 10", (int)CLI::ExitCodes::RequiredError},
         {"invert -o x --term-count 10", (int)CLI::ExitCodes::RequiredError}}));
    CAPTURE(args);
    auto argvec = split(args);
    auto argv = transform(argvec);
    auto settings = InvertSettings::parse(argvec.size(), &argv[0]);
    REQUIRE(std::get_if<int>(&settings) != nullptr);
    REQUIRE(*std::get_if<int>(&settings) == exit_code);
}
