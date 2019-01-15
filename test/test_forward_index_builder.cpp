#define CATCH_CONFIG_MAIN

#include <cstdio>
#include <string>

#include "Porter2/Porter2.hpp"
#include "boost/filesystem.hpp"
#include "catch2/catch.hpp"
#include "gsl/span"
#include "warcpp/warcpp.hpp"

#include "ds2i_config.hpp"
#include "enumerate.hpp"
#include "filesystem.hpp"
#include "forward_index_builder.hpp"
#include "parsing/html.hpp"
#include "temporary_directory.hpp"

using namespace boost::filesystem;

TEST_CASE("Batch file name", "[parsing][forward_index]")
{
    std::string basename = "basename";
    REQUIRE(pisa::Forward_Index_Builder<pisa::Plaintext_Record>::batch_file(basename, 0) ==
            basename + ".batch.0");
    REQUIRE(pisa::Forward_Index_Builder<pisa::Plaintext_Record>::batch_file(basename, 10) ==
            basename + ".batch.10");
}

TEST_CASE("Write document to stream", "[parsing][forward_index]")
{
    std::ostringstream os;

    auto [term_ids, encoded_sequence] = GENERATE(table<std::vector<uint32_t>, std::string>(
        {{{0, 1, 2, 3, 4, 3, 2, 1, 0},
          {9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
           4, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
         {{}, {0, 0, 0, 0}}}));
    WHEN("List of term IDs is written to stream") {
        pisa::Forward_Index_Builder<pisa::Plaintext_Record>::write_document(
            os, term_ids.begin(), term_ids.end());
        THEN("Encoded sequence is " << encoded_sequence) { REQUIRE(os.str() == encoded_sequence); }
    }
}

TEST_CASE("Write header", "[parsing][forward_index]")
{
    std::ostringstream os;

    auto [document_count, encoded_header] = GENERATE(table<uint32_t, std::string>(
        {{0, {1, 0, 0, 0, 0, 0, 0, 0}},
         {1, {1, 0, 0, 0, 1, 0, 0, 0}},
         {10, {1, 0, 0, 0, 10, 0, 0, 0}}}));
    GIVEN("Document count is " << document_count)
    WHEN("Header is written to stream") {
        pisa::Forward_Index_Builder<pisa::Plaintext_Record>::write_header(os, document_count);
        THEN("Encoded header is " << encoded_header) { REQUIRE(os.str() == encoded_header); }
    }
}

[[nodiscard]] std::vector<std::string> load_lines(std::istream &is) {
    std::string line;
    std::vector<std::string> vec;
    while (std::getline(is, line)) {
        vec.push_back(std::move(line));
    }
    return vec;
}

[[nodiscard]] std::vector<std::string> load_lines(std::string const &filename) {
    std::ifstream is(filename);
    return load_lines(is);
}

template <typename T>
void write_lines(std::ostream &os, gsl::span<T> &&elements)
{
    for (auto const& element : elements) {
        os << element << '\n';
    }
}

template <typename T>
void write_lines(std::string const &filename, gsl::span<T> &&elements)
{
    std::ofstream os(filename);
    write_lines<T>(os, std::forward<gsl::span<T>>(elements));
}

TEST_CASE("Build forward index batch", "[parsing][forward_index]")
{
    auto identity = [](std::string const &term) -> std::string { return term; };

    GIVEN("a few test records") {
        std::vector<pisa::Plaintext_Record> records{
            {"Doc10", "lorem ipsum dolor sit amet consectetur adipiscing elit"},
            {"Doc11", "integer rutrum felis et sagittis dapibus"},
            {"Doc12", "vivamus ac velit nec purus molestie tincidunt"},
            {"Doc13", "vivamus eu quam vitae lacus porta tempus quis eu metus"},
            {"Doc14", "curabitur a justo vitae turpis feugiat molestie eu ac nunc"}};
        WHEN("write a batch to temp directory") {
            Temporary_Directory tmpdir;
            auto output_file = tmpdir.path() / "fwd";
            pisa::Forward_Index_Builder<pisa::Plaintext_Record>::Batch_Process bp{
                7, records, pisa::Document_Id{10}, output_file.string()};
            pisa::Forward_Index_Builder<pisa::Plaintext_Record> builder;
            builder.run(bp, identity, pisa::parse_plaintext_content);
            THEN("documents are in check") {
                std::vector<std::string> expected_documents{
                    "Doc10", "Doc11", "Doc12", "Doc13", "Doc14"};
                auto documents = load_lines(output_file.string() + ".batch.7.documents");
                REQUIRE(documents == expected_documents);
            }
            THEN("terms are in check") {
                std::vector<std::string> expected_terms{
                    "lorem",      "ipsum",    "dolor",     "sit",     "amet",  "consectetur",
                    "adipiscing", "elit",     "integer",   "rutrum",  "felis", "et",
                    "sagittis",   "dapibus",  "vivamus",   "ac",      "velit", "nec",
                    "purus",      "molestie", "tincidunt", "eu",      "quam",  "vitae",
                    "lacus",      "porta",    "tempus",    "quis",    "metus", "curabitur",
                    "a",          "justo",    "turpis",    "feugiat", "nunc"};
                auto terms = load_lines(output_file.string() + ".batch.7.terms");
                REQUIRE(terms == expected_terms);
            }
            THEN("term IDs") {
                pisa::binary_collection coll((output_file.string() + ".batch.7").c_str());
                std::vector<std::vector<uint32_t>> documents;
                for (auto seq_iter = ++coll.begin(); seq_iter != coll.end(); ++seq_iter) {
                    auto seq = *seq_iter;
                    documents.emplace_back(seq.begin(), seq.end());
                }
                std::vector<std::vector<uint32_t>> expected_documents = {
                    {0, 1, 2, 3, 4, 5, 6, 7},
                    {8, 9, 10, 11, 12, 13},
                    {14, 15, 16, 17, 18, 19, 20},
                    {14, 21, 22, 23, 24, 25, 26, 27, 21, 28},
                    {29, 30, 31, 23, 32, 33, 19, 21, 15, 34}};
                REQUIRE(documents == expected_documents);
            }
        }
    }
}

void write_batch(std::string const &                       basename,
                 std::vector<std::string> const &          documents,
                 std::vector<std::string> const &          terms,
                 std::vector<std::vector<uint32_t>> const &collection)
{
    std::string document_file = basename + ".documents";
    std::string term_file     = basename + ".terms";
    write_lines(document_file, gsl::make_span(documents));
    write_lines(term_file, gsl::make_span(terms));
    std::ofstream os(basename);
    pisa::Forward_Index_Builder<pisa::Plaintext_Record>::write_header(os, collection.size());
    for (auto const& seq : collection) {
        pisa::Forward_Index_Builder<pisa::Plaintext_Record>::write_document(
            os, seq.begin(), seq.end());
    }
}

TEST_CASE("Merge forward index batches", "[parsing][forward_index]")
{
    Temporary_Directory tmpdir;
    auto dir = tmpdir.path();
    GIVEN("Three batches on disk") {
        std::vector<path> batch_paths{dir / "fwd.batch.0", dir / "fwd.batch.1", dir / "fwd.batch.2"};
        write_batch(batch_paths[0].string(),
                    {"Doc10", "Doc11"},
                    {"lorem",
                     "ipsum",
                     "dolor",
                     "sit",
                     "amet",
                     "consectetur",
                     "adipiscing",
                     "elit",
                     "integer",
                     "rutrum",
                     "felis",
                     "et",
                     "sagittis",
                     "dapibus"},
                    {{0, 1, 2, 3, 4, 5, 6, 7}, {8, 9, 10, 11, 12, 13}});
        write_batch(batch_paths[1].string(),
                    {"Doc12", "Doc13"},
                    {"vivamus",
                     "ac",
                     "velit",
                     "nec",
                     "purus",
                     "molestie",
                     "tincidunt",
                     "eu",
                     "quam",
                     "vitae",
                     "lacus",
                     "porta",
                     "tempus",
                     "quis",
                     "metus"},
                    {{0, 1, 2, 3, 4, 5, 6}, {0, 7, 8, 9, 10, 11, 12, 13, 7, 14}});
        write_batch(batch_paths[2].string(),
                    {"Doc14"},
                    {"curabitur",
                     "a",
                     "justo",
                     "vitae",
                     "turpis",
                     "feugiat",
                     "molestie",
                     "eu",
                     "ac",
                     "nunc"},
                    {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}});

        WHEN("Merging function is called") {
            auto output_file = (dir / "fwd").string();
            pisa::Forward_Index_Builder<pisa::Plaintext_Record> builder;
            builder.merge(output_file, 5, 3);

            THEN("documents are in check") {
                std::vector<std::string> expected_documents{
                    "Doc10", "Doc11", "Doc12", "Doc13", "Doc14"};
                auto documents = load_lines(output_file + ".documents");
                REQUIRE(documents == expected_documents);
            }
            THEN("terms are in check") {
                std::vector<std::string> expected_terms{
                    "a",         "ac",       "adipiscing", "amet",     "consectetur", "curabitur",
                    "dapibus",   "dolor",    "elit",       "et",       "eu",          "felis",
                    "feugiat",   "integer",  "ipsum",      "justo",    "lacus",       "lorem",
                    "metus",     "molestie", "nec",        "nunc",     "porta",       "purus",
                    "quam",      "quis",     "rutrum",     "sagittis", "sit",         "tempus",
                    "tincidunt", "turpis",   "velit",      "vitae",    "vivamus"};
                auto terms = load_lines(output_file + ".terms");
                REQUIRE(terms == expected_terms);
            }
            THEN("term IDs") {
                pisa::binary_collection coll((output_file).c_str());
                std::vector<std::vector<uint32_t>> documents;
                for (auto seq_iter = ++coll.begin(); seq_iter != coll.end(); ++seq_iter) {
                    auto seq = *seq_iter;
                    documents.emplace_back(seq.begin(), seq.end());
                }
                std::vector<std::vector<uint32_t>> expected_documents = {
                    {17, 14, 7, 28, 3, 4, 2, 8},
                    {13, 26, 11, 9, 27, 6},
                    {34, 1, 32, 20, 23, 19, 30},
                    {34, 10, 24, 33, 16, 22, 29, 25, 10, 18},
                    {5, 0, 15, 33, 31, 12, 19, 10, 1, 21}};
                REQUIRE(documents == expected_documents);
            }
        }
    }
}
TEST_CASE("Parse HTML content", "[parsing][forward_index][unit]")
{
    std::vector<std::string> vec;
    auto map_word = [&](std::string &&word) { vec.push_back(word); };
    SECTION("empty") {
        pisa::parse_html_content("<a/>", map_word);
        REQUIRE(vec == std::vector<std::string>{});
    }
    SECTION("non-empty") {
        pisa::parse_html_content("<a>lorem</a>ipsum", map_word);
        REQUIRE(vec == std::vector<std::string>{"lorem", "ipsum"});
    }
}

[[nodiscard]] auto load_term_map(std::string const& basename) -> std::vector<std::string> {
    std::vector<std::string> map;
    std::ifstream            is(basename + ".terms");
    std::string              str;
    while (std::getline(is, str)) {
        map.push_back(str);
    }
    return map;
}

TEST_CASE("Build forward index", "[parsing][forward_index][integration]")
{
    auto next_record = [](std::istream &in) -> std::optional<pisa::Plaintext_Record> {
        pisa::Plaintext_Record record;
        if (in >> record) {
            return record;
        }
        return std::nullopt;
    };

    GIVEN("A plaintext collection file") {
        std::string input(DS2I_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
        REQUIRE(boost::filesystem::exists(boost::filesystem::path(input)) == true);
        int thread_count = GENERATE(2, 8);
        int batch_size   = GENERATE(123, 10000);
        WHEN("Build a forward index") {
            Temporary_Directory tmpdir;
            auto dir = tmpdir.path();
            std::string output = (dir / "fwd").string();

            std::ifstream is(input);
            pisa::Forward_Index_Builder<pisa::Plaintext_Record> builder;
            builder.build(
                is,
                output,
                next_record,
                [](std::string &&term) -> std::string { return std::forward<std::string>(term); },
                pisa::parse_plaintext_content,
                batch_size,
                thread_count);

            THEN("The collection mapped to terms matches input") {
                auto term_map = load_term_map(output);
                pisa::binary_collection coll((output).c_str());
                auto seq_iter = coll.begin();
                REQUIRE(*seq_iter->begin() == 10000);
                ++seq_iter;
                std::ifstream plain_is(input);
                std::optional<pisa::Plaintext_Record> record = std::nullopt;
                while ((record = next_record(plain_is)).has_value()) {
                    std::vector<std::string> original_body;
                    std::istringstream content_stream(record->content());
                    std::string term;
                    while (content_stream >> term) {
                        original_body.push_back(std::move(term));
                    }
                    std::vector<std::string> produced_body;
                    for (auto term_id : *seq_iter) {
                        produced_body.push_back(term_map[term_id]);
                    }
                    REQUIRE(produced_body == original_body);
                    ++seq_iter;
                }
                auto batch_files = pisa::ls(dir, [](auto const &filename) {
                    return filename.find("batch") != std::string::npos;
                });
                REQUIRE(batch_files.empty());
            }
        }
    }
}

TEST_CASE("Build forward index (WARC)", "[.][parsing][forward_index][integration]")
{
    auto next_record = [](std::istream &in) -> std::optional<warcpp::Warc_Record> {
        warcpp::Warc_Record record;
        if (read_warc_record(in, record)) {
            return std::make_optional(record);
        }
        return std::nullopt;
    };
    auto process_term = [&](std::string &&term) -> std::string {
        std::transform(term.begin(),
                       term.end(),
                       term.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return stem::Porter2{}.stem(term);
    };
    auto next_plain_record = [](std::istream &in) -> std::optional<pisa::Plaintext_Record> {
        pisa::Plaintext_Record record;
        if (in >> record) {
            return record;
        }
        return std::nullopt;
    };

    GIVEN("A plaintext collection file") {
        std::string input(DS2I_SOURCE_DIR "/test/test_data/clueweb1k.warc");
        REQUIRE(boost::filesystem::exists(boost::filesystem::path(input)) == true);
        int thread_count = 2;
        int batch_size   = 123;
        WHEN("Build a forward index") {
            Temporary_Directory tmpdir;
            auto dir = tmpdir.path();
            std::string output = (dir / "fwd").string();

            std::ifstream is(input);
            pisa::Forward_Index_Builder<warcpp::Warc_Record> builder;
            builder.build(is,
                          output,
                          next_record,
                          process_term,
                          pisa::parse_html_content,
                          batch_size,
                          thread_count);

            THEN("The collection mapped to terms matches input") {
                auto term_map = load_term_map(output);
                pisa::binary_collection coll((output).c_str());
                auto seq_iter = coll.begin();
                CHECK(*seq_iter->begin() == 10000);
                ++seq_iter;
                std::ifstream plain_is(DS2I_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
                std::ifstream doc_is(output + ".documents");
                std::optional<pisa::Plaintext_Record> record = std::nullopt;
                while ((record = next_plain_record(plain_is)).has_value()) {
                    std::string doc;
                    std::getline(doc_is, doc);
                    REQUIRE(doc == record->trecid());
                    std::vector<std::string> original_body;
                    std::istringstream content_stream(record->content());
                    std::string term;
                    while (content_stream >> term) {
                        original_body.push_back(std::move(term));
                    }
                    std::vector<std::string> produced_body;
                    for (auto term_id : *seq_iter) {
                        produced_body.push_back(term_map[term_id]);
                    }
                    std::cerr << doc << '\n';
                    CHECK(produced_body == original_body);
                    ++seq_iter;
                }
                auto batch_files = pisa::ls(dir, [](auto const &filename) {
                    return filename.find("batch") != std::string::npos;
                });
                REQUIRE(batch_files.empty());
            }
        }
    }
}
