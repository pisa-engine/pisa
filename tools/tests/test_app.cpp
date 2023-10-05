#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <cstring>

#include <CLI/CLI.hpp>

#include "../app.hpp"
#include "io.hpp"
#include "payload_vector.hpp"
#include "temporary_directory.hpp"
#include "wand_utils.hpp"

/**
 * A wrapper constructing and destroying an argument list, to pass to CLI parse
 * function, which only supports passing raw char** but not vectors.
 */
struct Arguments {
    int argc;
    char** argv;

    explicit Arguments(std::vector<std::string> const& args)
        : argc(args.size() + 1), argv(new char*[argc])
    {
        std::string name = "<executable>";
        argv[0] = new char[name.size() + 1];
        std::strcpy(argv[0], name.c_str());
        for (int i = 1; i < argc; ++i) {
            argv[i] = new char[args[i - 1].size() + 1];
            std::strcpy(argv[i], args[i - 1].c_str());
        }
    }
    Arguments(Arguments const&) = delete;
    Arguments& operator=(Arguments const&) = delete;
    Arguments(Arguments&&) = default;
    Arguments& operator=(Arguments&&) = default;
    ~Arguments()
    {
        if (argv != nullptr) {
            while (argc-- > 0) {
                delete[] argv[argc];
            }
            delete[] argv;
        }
    }
};

/**
 * Parses the CLI input using the given arguments.
 *
 * Usually, we would use CLI_PARSE macro that is a handy shortcut for exiting
 * when parsing fails, but here we need more control to catch potential errors,
 * so we call `CLI::App::parse` method directly.
 */
void parse(CLI::App& app, std::vector<std::string> const& args)
{
    ::Arguments input(args);
    app.parse(input.argc, input.argv);
}

TEST_CASE("Encoding", "[cli]")
{
    CLI::App app("Encoding test");
    pisa::Args<pisa::arg::Encoding> args(&app);
    SECTION("No arguments throws") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Check encoding long option")
    {
        // Note that we currently don't validate passed encoding until we use it,
        // so any string is valid at this point.
        REQUIRE_NOTHROW(parse(app, {"--encoding", "ENCODING"}));
        REQUIRE(args.index_encoding() == "ENCODING");
    }
    SECTION("Check encoding short option")
    {
        REQUIRE_NOTHROW(parse(app, {"-e", "ENCODING"}));
        REQUIRE(args.index_encoding() == "ENCODING");
    }
}

TEST_CASE("WandData", "[cli]")
{
    CLI::App app("WandData test");
    WHEN("WAND data is optional")
    {
        pisa::Args<pisa::arg::WandData<pisa::arg::WandMode::Optional>> args(&app);
        SECTION("No arguments is OK")
        {
            REQUIRE_NOTHROW(parse(app, {}));
            REQUIRE_FALSE(args.wand_data_path().has_value());
        }
        SECTION("Defined with long option")
        {
            REQUIRE_NOTHROW(parse(app, {"--wand", "WDATA"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE_FALSE(args.is_wand_compressed());
        }
        SECTION("Defined with short option")
        {
            REQUIRE_NOTHROW(parse(app, {"-w", "WDATA"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE_FALSE(args.is_wand_compressed());
        }
        SECTION("Compressed")
        {
            REQUIRE_NOTHROW(parse(app, {"-w", "WDATA", "--compressed-wand"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE(args.is_wand_compressed());
        }
    }
    WHEN("WAND data is required")
    {
        pisa::Args<pisa::arg::WandData<pisa::arg::WandMode::Required>> args(&app);
        SECTION("No arguments throws") { REQUIRE_THROWS(parse(app, {})); }
        SECTION("Defined with long option")
        {
            REQUIRE_NOTHROW(parse(app, {"--wand", "WDATA"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE_FALSE(args.is_wand_compressed());
        }
        SECTION("Defined with short option")
        {
            REQUIRE_NOTHROW(parse(app, {"-w", "WDATA"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE_FALSE(args.is_wand_compressed());
        }
        SECTION("Compressed")
        {
            REQUIRE_NOTHROW(parse(app, {"-w", "WDATA", "--compressed-wand"}));
            REQUIRE(args.wand_data_path() == "WDATA");
            REQUIRE(args.is_wand_compressed());
        }
    }
}

TEST_CASE("Index", "[cli]")
{
    CLI::App app("Index test");
    pisa::Args<pisa::arg::Index> args(&app);
    SECTION("No arguments throws") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Only encoding throws") { REQUIRE_THROWS(parse(app, {"--encoding", "ENCODING"})); }
    SECTION("Only index throws") { REQUIRE_THROWS(parse(app, {"--index", "INDEX"})); }
    SECTION("Check long options")
    {
        REQUIRE_NOTHROW(parse(app, {"--encoding", "ENCODING", "--index", "INDEX"}));
        REQUIRE(args.index_encoding() == "ENCODING");
        REQUIRE(args.index_filename() == "INDEX");
    }
    SECTION("Check short options")
    {
        REQUIRE_NOTHROW(parse(app, {"-e", "ENCODING", "-i", "INDEX"}));
        REQUIRE(args.index_encoding() == "ENCODING");
        REQUIRE(args.index_filename() == "INDEX");
    }
}

/**
 * Verifies that the given tokenizer produces the expected stream of tokens on the given input.
 */
void test_tokenizer(
    std::unique_ptr<pisa::Tokenizer> const& tokenizer,
    std::string_view input,
    std::vector<std::string> const& expected)
{
    auto stream = tokenizer->tokenize(input);
    std::vector<std::string> actual(stream->begin(), stream->end());
    REQUIRE(actual == expected);
}

/**
 * Verifies that the given analyzer produces the expected stream of tokens on the given input.
 */
void test_analyzer(
    pisa::TextAnalyzer const& tokenizer,
    std::string_view input,
    std::vector<std::string> const& expected)
{
    auto stream = tokenizer.analyze(input);
    std::vector<std::string> actual(stream->begin(), stream->end());
    REQUIRE(actual == expected);
}

void write_lines(std::filesystem::path const& path, std::vector<std::string> const& lines)
{
    std::ofstream out(path.c_str());
    for (auto const& line: lines) {
        out << line << '\n';
    }
}

TEST_CASE("Analyzer", "[cli]")
{
    CLI::App app("Analyzer test");
    pisa::Args<pisa::arg::Analyzer> args(&app);
    SECTION("Defaults")
    {
        parse(app, {});
        test_tokenizer(args.tokenizer(), "A's b c's", {"A", "b", "c"});
        test_analyzer(args.text_analyzer(), "A's b c's", {"A", "b", "c"});
    }
    SECTION("English tokenizer")
    {
        parse(app, {});
        test_tokenizer(args.tokenizer(), "A's b c's", {"A", "b", "c"});
        test_analyzer(args.text_analyzer(), "A's b c's", {"A", "b", "c"});
    }
    SECTION("Whitespace tokenizer")
    {
        parse(app, {"--tokenizer", "whitespace"});
        test_tokenizer(args.tokenizer(), "A's b c's", {"A's", "b", "c's"});
        test_analyzer(args.text_analyzer(), "A's b c's", {"A's", "b", "c's"});
    }
    SECTION("Lowercase")
    {
        parse(app, {"--token-filters", "lowercase"});
        test_tokenizer(args.tokenizer(), "A's b c's", {"A", "b", "c"});
        test_analyzer(args.text_analyzer(), "A's b c's", {"a", "b", "c"});
    }
    SECTION("Lowercase & Porter2")
    {
        parse(app, {"--token-filters", "lowercase", "porter2"});
        test_tokenizer(args.tokenizer(), "A's b c's flying", {"A", "b", "c", "flying"});
        test_analyzer(args.text_analyzer(), "A's b c's flying", {"a", "b", "c", "fli"});
    }
    SECTION("Krovetz")
    {
        parse(app, {"--token-filters", "krovetz"});
        test_tokenizer(
            args.tokenizer(), "A's b c's flying playing", {"A", "b", "c", "flying", "playing"});
        // Note: Krovetz seems to: (a) lowercase, and (b) not stem words like "flying"
        test_analyzer(
            args.text_analyzer(), "A's b c's flying playing", {"a", "b", "c", "flying", "play"});
    }
    SECTION("Stopwords")
    {
        pisa::TemporaryDirectory dir;
        auto stopwords_path = dir.path() / "stopwords.txt";
        write_lines(
            stopwords_path,
            {"fli",  // Adding stemmed to make sure stopwords are removed at the end
             "b"});

        parse(
            app,
            {"--stopwords", stopwords_path.string(), "--token-filters", "lowercase", "porter2"});
        test_tokenizer(args.tokenizer(), "A's b c's flying", {"A", "b", "c", "flying"});
        test_analyzer(args.text_analyzer(), "A's b c's flying", {"a", "c"});
    }
}

TEST_CASE("Ranked Query", "[cli]")
{
    CLI::App app("Analyzer test");
    pisa::Args<pisa::arg::Query<>> args(&app);

    SECTION("Throws without arguments -- queries are required") { REQUIRE_THROWS(parse(app, {})); }

    GIVEN("Non-existend input file path")
    {
        auto queries = std::filesystem::path("queries.txt");
        THEN("File cannot be read and it throws")
        {
            REQUIRE_THROWS(parse(app, {"--queries", queries.string()}));
        }
    }

    GIVEN("Input file with term IDs and query IDs")
    {
        pisa::TemporaryDirectory dir;

        auto queries = dir.path() / "queries.txt";
        write_lines(queries, {"1:0 1 2", "3 4 5" /* missing qid */, "3:6 7 8"});

        auto terms = dir.path() / "terms.txt";
        // clang-format off
        std::vector<std::string> term_vector = {
            "0", "00", "1", "11", "2", "22", "3", "33", "4", "44",
            "5", "55", "6", "66", "7", "77" /* 8 is missing! */};
        // clang-format on
        auto termlex = pisa::encode_payload_vector(term_vector.begin(), term_vector.end());
        termlex.to_file(terms.string());

        WHEN("Only query file is provided")
        {
            std::vector<std::string> input{"--queries", queries.string()};
            THEN("Fails due to missing k") { REQUIRE_THROWS(parse(app, input)); }
        }

        WHEN("Query file and k are provided")
        {
            std::vector<std::string> input{"--queries", queries.string(), "-k", "100"};
            THEN("Parses queries using term IDs")
            {
                parse(app, input);
                auto qs = args.queries();
                REQUIRE(qs.size() == 3);

                REQUIRE(qs[0].id == "1");
                REQUIRE(qs[0].terms == std::vector<std::uint32_t>{0, 1, 2});
                REQUIRE(qs[0].term_weights.empty());

                REQUIRE(qs[1].id == std::nullopt);
                REQUIRE(qs[1].terms == std::vector<std::uint32_t>{3, 4, 5});
                REQUIRE(qs[1].term_weights.empty());

                REQUIRE(qs[2].id == "3");
                REQUIRE(qs[2].terms == std::vector<std::uint32_t>{6, 7, 8});
                REQUIRE(qs[2].term_weights.empty());
            }
        }

        WHEN("Query file, k, and terms are provided")
        {
            std::vector<std::string> input{
                "--queries", queries.string(), "-k", "100", "--terms", terms.string()};
            THEN("Considers numbers from input as strings")
            {
                parse(app, input);
                auto qs = args.queries();
                REQUIRE(qs.size() == 3);

                REQUIRE(qs[0].id == "1");
                REQUIRE(qs[0].terms == std::vector<std::uint32_t>{0, 2, 4});
                REQUIRE(qs[0].term_weights.empty());

                REQUIRE(qs[1].id == std::nullopt);
                REQUIRE(qs[1].terms == std::vector<std::uint32_t>{6, 8, 10});
                REQUIRE(qs[1].term_weights.empty());

                REQUIRE(qs[2].id == "3");
                REQUIRE(qs[2].terms == std::vector<std::uint32_t>{12, 14});
                REQUIRE(qs[2].term_weights.empty());
            }
        }
    }

    GIVEN("Input file with terms and query IDs")
    {
        pisa::TemporaryDirectory dir;

        auto queries = dir.path() / "queries.txt";
        write_lines(
            queries, {"1:dog dog dog", "dog cat mouse" /* missing qid */, "3:pelican moose"});

        auto terms = dir.path() / "terms.txt";
        std::vector<std::string> term_vector = {
            "cat",  // 0
            "dog",  // 1
            /* moose is missing! */
            "mouse",  // 2
            "pelican"  // 3
        };
        auto termlex = pisa::encode_payload_vector(term_vector.begin(), term_vector.end());
        termlex.to_file(terms.string());

        WHEN("Query file and k are provided")
        {
            std::vector<std::string> input{"--queries", queries.string(), "-k", "100"};
            THEN("Fails to parse term IDs in the file")
            {
                parse(app, input);
                REQUIRE_THROWS(args.queries());
            }
        }

        WHEN("Query file, k, and terms are provided")
        {
            std::vector<std::string> input{
                "--queries", queries.string(), "-k", "100", "--terms", terms.string()};
            THEN("Translates the terms into IDs")
            {
                parse(app, input);
                auto qs = args.queries();
                REQUIRE(qs.size() == 3);

                REQUIRE(qs[0].id == "1");
                REQUIRE(qs[0].terms == std::vector<std::uint32_t>{1, 1, 1});
                REQUIRE(qs[0].term_weights.empty());

                REQUIRE(qs[1].id == std::nullopt);
                REQUIRE(qs[1].terms == std::vector<std::uint32_t>{1, 0, 2});
                REQUIRE(qs[1].term_weights.empty());

                REQUIRE(qs[2].id == "3");
                REQUIRE(qs[2].terms == std::vector<std::uint32_t>{3});
                REQUIRE(qs[2].term_weights.empty());
            }
        }

        // If this behavior surprises you, see https://github.com/pisa-engine/pisa/issues/501
        WHEN("Query file, k, and terms are provided, and --weighted is passed")
        {
            std::vector<std::string> input{
                "--queries", queries.string(), "-k", "100", "--terms", terms.string(), "--weighted"};
            THEN("Translates the terms into IDs")
            {
                parse(app, input);
                auto qs = args.queries();
                REQUIRE(qs.size() == 3);

                REQUIRE(qs[0].id == "1");
                REQUIRE(qs[0].terms == std::vector<std::uint32_t>{1, 1, 1});
                REQUIRE(qs[0].term_weights.empty());

                REQUIRE(qs[1].id == std::nullopt);
                REQUIRE(qs[1].terms == std::vector<std::uint32_t>{1, 0, 2});
                REQUIRE(qs[1].term_weights.empty());

                REQUIRE(qs[2].id == "3");
                REQUIRE(qs[2].terms == std::vector<std::uint32_t>{3});
                REQUIRE(qs[2].term_weights.empty());

                REQUIRE(args.weighted());
            }
        }
    }
}

TEST_CASE("Algorithm", "[cli]")
{
    CLI::App app("Algorithm test");
    pisa::Args<pisa::arg::Algorithm> args(&app);
    SECTION("No arguments throws") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Long option")
    {
        // Note: algorithm names are not validated until later.
        parse(app, {"--algorithm", "ALG"});
        REQUIRE(args.algorithm() == "ALG");
    }
    SECTION("Short option")
    {
        // Note: algorithm names are not validated until later.
        parse(app, {"-a", "ALG"});
        REQUIRE(args.algorithm() == "ALG");
    }
}

TEST_CASE("Scorer", "[cli]")
{
    CLI::App app("Scorer test");
    pisa::Args<pisa::arg::Scorer> args(&app);
    SECTION("No arguments throws") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Long option with defaults")
    {
        parse(app, {"--scorer", "scorer"});
        auto params = args.scorer_params();
        REQUIRE(params.name == "scorer");
        REQUIRE(params.bm25_b == 0.4F);
        REQUIRE(params.bm25_k1 == 0.9F);
        REQUIRE(params.pl2_c == 1.0F);
        REQUIRE(params.qld_mu == 1000.0F);
    }
    SECTION("Short option")
    {
        parse(
            app,
            {"-s",
             "scorer",
             "--bm25-b",
             "0.5",
             "--bm25-k1",
             "1.0",
             "--pl2-c",
             "1.1",
             "--qld-mu",
             "1001"});
        auto params = args.scorer_params();
        REQUIRE(params.name == "scorer");
        REQUIRE(params.bm25_b == 0.5F);
        REQUIRE(params.bm25_k1 == 1.0F);
        REQUIRE(params.pl2_c == 1.1F);
        REQUIRE(params.qld_mu == 1001.0F);
    }
}

TEST_CASE("Quantize", "[cli]")
{
    CLI::App app("Scorer test");
    pisa::Args<pisa::arg::Quantize> args(&app);
    SECTION("Scorer without --quantize throws")
    {
        REQUIRE_THROWS(parse(app, {"--scorer", "scorer"}));
    }
    SECTION("Wand without --quantize throws") { REQUIRE_THROWS(parse(app, {"--wand", "WAND"})); }
    SECTION("Long scorer & wand options with defaults")
    {
        parse(app, {"--quantize", "--scorer", "scorer", "--wand", "WAND"});
        auto params = args.scorer_params();
        REQUIRE(params.name == "scorer");
        REQUIRE(params.bm25_b == 0.4F);
        REQUIRE(params.bm25_k1 == 0.9F);
        REQUIRE(params.pl2_c == 1.0F);
        REQUIRE(params.qld_mu == 1000.0F);
    }
    SECTION("Short scorer & wand options")
    {
        parse(
            app,
            {"--quantize",
             "-s",
             "scorer",
             "--bm25-b",
             "0.5",
             "--bm25-k1",
             "1.0",
             "--pl2-c",
             "1.1",
             "--qld-mu",
             "1001",
             "--wand",
             "WAND"});
        auto params = args.scorer_params();
        REQUIRE(params.name == "scorer");
        REQUIRE(params.bm25_b == 0.5F);
        REQUIRE(params.bm25_k1 == 1.0F);
        REQUIRE(params.pl2_c == 1.1F);
        REQUIRE(params.qld_mu == 1001.0F);
    }
}

TEST_CASE("Thresholds", "[cli]")
{
    CLI::App app("Thresholds test");
    pisa::Args<pisa::arg::Thresholds> args(&app);
    SECTION("Long option")
    {
        parse(app, {"--thresholds", "THRESHOLDS"});
        REQUIRE(args.thresholds_file() == "THRESHOLDS");
    }
    SECTION("Short option")
    {
        parse(app, {"-T", "THRESHOLDS"});
        REQUIRE(args.thresholds_file() == "THRESHOLDS");
    }
}

TEST_CASE("Verbose", "[cli]")
{
    CLI::App app("Verbose test");
    pisa::Args<pisa::arg::Verbose> args(&app);
    SECTION("By default not verbose")
    {
        parse(app, {});
        REQUIRE_FALSE(args.verbose());
    }
    SECTION("Long option")
    {
        parse(app, {"--verbose"});
        REQUIRE(args.verbose());
    }
    SECTION("Short option")
    {
        parse(app, {"-v"});
        REQUIRE(args.verbose());
    }
}

TEST_CASE("Threads", "[cli]")
{
    CLI::App app("Threads test");
    pisa::Args<pisa::arg::Threads> args(&app);
    SECTION("By default equal to hardware concurrency")
    {
        parse(app, {});
        REQUIRE(args.threads() == std::thread::hardware_concurrency());
    }
    SECTION("Long option")
    {
        parse(app, {"--threads", "10"});
        REQUIRE(args.threads() == 10);
    }
    SECTION("Short option")
    {
        parse(app, {"-j", "10"});
        REQUIRE(args.threads() == 10);
    }
}

TEST_CASE("Batch size", "[cli]")
{
    CLI::App app("Batch size test");
    pisa::Args<pisa::arg::BatchSize<100>> args(&app);
    SECTION("Default")
    {
        parse(app, {});
        REQUIRE(args.batch_size() == 100);
    }
    SECTION("Long option")
    {
        parse(app, {"--batch-size", "200"});
        REQUIRE(args.batch_size() == 200);
    }
}

TEST_CASE("Invert", "[cli]")
{
    CLI::App app("Invert test");
    pisa::Args<pisa::arg::Invert> args(&app);
    SECTION("Throws without arguments") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Throws with input only") { REQUIRE_THROWS(parse(app, {"--input", "INPUT"})); }
    SECTION("Throws with output only") { REQUIRE_THROWS(parse(app, {"--output", "OUTPUT"})); }
    SECTION("Short options")
    {
        parse(app, {"-i", "INPUT", "-o", "OUTPUT"});
        REQUIRE(args.input_basename() == "INPUT");
        REQUIRE(args.output_basename() == "OUTPUT");
        REQUIRE(args.term_count() == std::nullopt);
    }
    SECTION("With term count")
    {
        parse(app, {"--input", "INPUT", "--output", "OUTPUT", "--term-count", "123"});
        REQUIRE(args.input_basename() == "INPUT");
        REQUIRE(args.output_basename() == "OUTPUT");
        REQUIRE(args.term_count() == 123);
    }
}

TEST_CASE("Compress", "[cli]")
{
    CLI::App app("Compress test");
    pisa::Args<pisa::arg::Compress> args(&app);
    SECTION("Throws without arguments") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Throws with collection only")
    {
        REQUIRE_THROWS(parse(app, {"--collection", "COLLECTION"}));
    }
    SECTION("Throws with output only") { REQUIRE_THROWS(parse(app, {"--output", "OUTPUT"})); }
    SECTION("Short options")
    {
        parse(app, {"-c", "COLLECTION", "-o", "OUTPUT"});
        REQUIRE(args.input_basename() == "COLLECTION");
        REQUIRE(args.output() == "OUTPUT");
        REQUIRE_FALSE(args.check());
    }
    SECTION("With check")
    {
        parse(app, {"--collection", "COLLECTION", "--output", "OUTPUT", "--check"});
        REQUIRE(args.input_basename() == "COLLECTION");
        REQUIRE(args.output() == "OUTPUT");
        REQUIRE(args.check());
    }
}

TEST_CASE("CreateWandData", "[cli]")
{
    CLI::App app("CreateWandData test");
    pisa::Args<pisa::arg::CreateWandData> args(&app);
    SECTION("Throws without arguments") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Throws without scorer")
    {
        REQUIRE_THROWS(
            parse(app, {"--collection", "COLLECTION", "--output", "OUTPUT", "--block-size", "10"}));
    }
    SECTION("Throws without collection")
    {
        REQUIRE_THROWS(
            parse(app, {"--scorer", "SCORER", "--output", "OUTPUT", "--block-size", "10"}));
    }
    SECTION("Throws without output")
    {
        REQUIRE_THROWS(
            parse(app, {"--scorer", "SCORER", "--collection", "COLLECTION", "--block-size", "10"}));
    }
    SECTION("Throws without block size or lambda")
    {
        REQUIRE_THROWS(
            parse(app, {"--collection", "COLLECTION", "--output", "OUTPUT", "--scorer", "SCORER"}));
    }
    SECTION("Defaults with block size")
    {
        parse(
            app,
            {"--collection",
             "COLLECTION",
             "--output",
             "OUTPUT",
             "--scorer",
             "SCORER",
             "--block-size",
             "10"});
        REQUIRE(args.input_basename() == "COLLECTION");
        REQUIRE(args.output() == "OUTPUT");
        REQUIRE(args.scorer_params().name == "SCORER");
        REQUIRE_FALSE(args.compress());
        REQUIRE_FALSE(args.quantize());
        REQUIRE_FALSE(args.range());
        REQUIRE(args.dropped_term_ids().empty());
        REQUIRE(std::get<pisa::FixedBlock>(args.block_size()).size == 10);
    }
    SECTION("With lambda and other options")
    {
        parse(
            app,
            {"--collection",
             "COLLECTION",
             "--output",
             "OUTPUT",
             "--scorer",
             "SCORER",
             "--lambda",
             "0.5",
             "--compress",
             "--quantize"});
        REQUIRE(args.input_basename() == "COLLECTION");
        REQUIRE(args.output() == "OUTPUT");
        REQUIRE(args.scorer_params().name == "SCORER");
        REQUIRE(args.compress());
        REQUIRE(args.quantize());
        REQUIRE_FALSE(args.range());
        REQUIRE(args.dropped_term_ids().empty());
        REQUIRE(std::get<pisa::VariableBlock>(args.block_size()).lambda == 0.5);
    }
    SECTION("With range")
    {
        // TODO: This currently does not work!
        //       See https://github.com/pisa-engine/pisa/issues/502
        // parse(
        //     app,
        //     {"--collection", "COLLECTION", "--output", "OUTPUT", "--scorer", "SCORER",
        //     "--range"});
        // REQUIRE(args.input_basename() == "COLLECTION");
        // REQUIRE(args.output() == "OUTPUT");
        // REQUIRE(args.scorer_params().name == "SCORER");
        // REQUIRE_FALSE(args.compress());
        // REQUIRE_FALSE(args.quantize());
        // REQUIRE(args.range());
        // REQUIRE(args.dropped_term_ids().empty());
    }
    SECTION("Terms to drop")
    {
        pisa::TemporaryDirectory dir;
        auto terms_to_drop_path = dir.path() / "terms_to_drop.txt";
        write_lines(terms_to_drop_path, {"1", "2", "3"});
        parse(
            app,
            {"--collection",
             "COLLECTION",
             "--output",
             "OUTPUT",
             "--scorer",
             "SCORER",
             "--block-size",
             "10",
             "--terms-to-drop",
             terms_to_drop_path.string()});
        REQUIRE(args.dropped_term_ids() == std::unordered_set<std::size_t>{1, 2, 3});
    }
}

TEST_CASE("ReorderDocuments", "[cli]")
{
    CLI::App app("ReorderDocuments test");
    pisa::Args<pisa::arg::ReorderDocuments> args(&app);
    SECTION("Throws without arguments") { REQUIRE_THROWS(parse(app, {})); }
    SECTION("Random")
    {
        parse(app, {"--collection", "INPUT", "--output", "OUTPUT", "--random"});
        REQUIRE(args.random());
        REQUIRE_FALSE(args.bp());
    }
    SECTION("Random seed")
    {
        parse(app, {"--collection", "INPUT", "--output", "OUTPUT", "--random", "--seed", "17"});
        REQUIRE(args.random());
        REQUIRE_FALSE(args.bp());
        REQUIRE(args.seed() == 17ULL);
    }
    SECTION("Reordered documents cannot be written if not passed")
    {
        REQUIRE_THROWS(parse(
            app,
            {"--collection",
             "INPUT",
             "--output",
             "OUTPUT",
             "--random",
             "--reordered-documents",
             "REORDERD"}));
    }
    SECTION("Reordered documents")
    {
        parse(
            app,
            {"--collection",
             "INPUT",
             "--output",
             "OUTPUT",
             "--random",
             "--documents",
             "DOCLEX",
             "--reordered-documents",
             "REORDERED"});
        REQUIRE(args.random());
        REQUIRE_FALSE(args.bp());
        REQUIRE(*args.reordered_document_lexicon() == "REORDERED");
    }
    SECTION("From mapping")
    {
        parse(app, {"--collection", "INPUT", "--output", "OUTPUT", "--from-mapping", "MAPPING"});
        REQUIRE_FALSE(args.random());
        REQUIRE_FALSE(args.bp());
        REQUIRE(*args.mapping_file() == "MAPPING");
    }
    SECTION("By feature")
    {
        parse(app, {"--collection", "INPUT", "--output", "OUTPUT", "--by-feature", "FEATURE"});
        REQUIRE_FALSE(args.random());
        REQUIRE_FALSE(args.bp());
        REQUIRE(*args.feature_file() == "FEATURE");
    }
    SECTION("BP")
    {
        parse(app, {"--collection", "INPUT", "--output", "OUTPUT", "--bp"});
        REQUIRE_FALSE(args.random());
        REQUIRE(args.bp());
    }
}

TEST_CASE("Separator", "[cli]")
{
    CLI::App app("Separator test");
    pisa::Args<pisa::arg::Separator> args(&app);
    SECTION("Default")
    {
        parse(app, {});
        REQUIRE(args.separator() == "\t");
    }
    SECTION("Defined")
    {
        parse(app, {"--sep", ","});
        REQUIRE(args.separator() == ",");
    }
}

TEST_CASE("PrintQueryId", "[cli]")
{
    CLI::App app("PrintQueryId test");
    pisa::Args<pisa::arg::PrintQueryId> args(&app);
    SECTION("Default")
    {
        parse(app, {});
        REQUIRE_FALSE(args.print_query_id());
    }
    SECTION("Defined")
    {
        parse(app, {"--query-id"});
        REQUIRE(args.print_query_id());
    }
}

TEST_CASE("LogLevel", "[cli]")
{
    CLI::App app("LogLevel test");
    pisa::Args<pisa::arg::LogLevel> args(&app);
    SECTION("Default is info")
    {
        parse(app, {});
        REQUIRE(args.log_level() == spdlog::level::level_enum::info);
    }
    SECTION("Error")
    {
        parse(app, {"--log-level", "err"});
        REQUIRE(args.log_level() == spdlog::level::level_enum::err);
    }
    SECTION("Debug")
    {
        parse(app, {"--log-level", "debug"});
        REQUIRE(args.log_level() == spdlog::level::level_enum::debug);
    }
}
