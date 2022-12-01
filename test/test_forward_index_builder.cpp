#define CATCH_CONFIG_MAIN

#include <algorithm>
#include <cstdio>
#include <string>

#include <boost/filesystem.hpp>
#include <catch2/catch.hpp>
#include <gsl/span>

#include "binary_collection.hpp"
#include "filesystem.hpp"
#include "forward_index_builder.hpp"
#include "parser.hpp"
#include "parsing/html.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "tokenizer.hpp"

using namespace boost::filesystem;
using namespace pisa;

TEST_CASE("Batch file name", "[parsing][forward_index]")
{
    std::string basename = "basename";
    REQUIRE(Forward_Index_Builder::batch_file(basename, 0) == basename + ".batch.0");
    REQUIRE(Forward_Index_Builder::batch_file(basename, 10) == basename + ".batch.10");
}

TEST_CASE("Write document to stream", "[parsing][forward_index]")
{
    std::ostringstream os;

    auto [term_ids, encoded_sequence] = GENERATE(table<std::vector<uint32_t>, std::string>(
        {{{0, 1, 2, 3, 4, 3, 2, 1, 0},
          {9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
           4, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
         {{}, {0, 0, 0, 0}}}));
    WHEN("List of term IDs is written to stream")
    {
        Forward_Index_Builder::write_document(os, term_ids.begin(), term_ids.end());
        THEN("Encoded sequence is " << encoded_sequence) { REQUIRE(os.str() == encoded_sequence); }
    }
}

TEST_CASE("Write header", "[parsing][forward_index]")
{
    std::ostringstream os;

    auto [document_count, encoded_header] =
        GENERATE(table<uint32_t, std::string>({{0, {1, 0, 0, 0, 0, 0, 0, 0}},
                                               {1, {1, 0, 0, 0, 1, 0, 0, 0}},
                                               {10, {1, 0, 0, 0, 10, 0, 0, 0}}}));
    GIVEN("Document count is " << document_count)
    WHEN("Header is written to stream")
    {
        Forward_Index_Builder::write_header(os, document_count);
        THEN("Encoded header is " << encoded_header) { REQUIRE(os.str() == encoded_header); }
    }
}

[[nodiscard]] std::vector<std::string> load_lines(std::istream& is)
{
    std::string line;
    std::vector<std::string> vec;
    while (std::getline(is, line)) {
        vec.push_back(std::move(line));
    }
    return vec;
}

[[nodiscard]] std::vector<std::string> load_lines(std::string const& filename)
{
    std::ifstream is(filename);
    return load_lines(is);
}

template <typename T>
void write_lines(std::ostream& os, gsl::span<T>&& elements)
{
    for (auto const& element: elements) {
        os << element << '\n';
    }
}

template <typename T>
void write_lines(std::string const& filename, gsl::span<T>&& elements)
{
    std::ofstream os(filename);
    write_lines<T>(os, std::forward<gsl::span<T>>(elements));
}

TEST_CASE("Build forward index batch", "[parsing][forward_index]")
{
    auto identity = [](std::string const& term) -> std::string { return term; };

    GIVEN("a few test records")
    {
        std::vector<Document_Record> records{
            Document_Record("Doc10", "lorem ipsum dolor sit amet consectetur adipiscing elit", ""),
            Document_Record("Doc11", "integer rutrum felis et sagittis dapibus", ""),
            Document_Record("Doc12", "vivamus ac velit nec purus molestie tincidunt", ""),
            Document_Record("Doc13", "vivamus eu quam vitae lacus porta tempus quis eu metus", ""),
            Document_Record(
                "Doc14", "curabitur a justo vitae turpis feugiat molestie eu ac nunc", "")};
        WHEN("write a batch to temp directory")
        {
            pisa::TemporaryDirectory tmpdir;
            auto output_file = tmpdir.path() / "fwd";
            Forward_Index_Builder::Batch_Process bp{
                7, records, Document_Id{10}, output_file.string()};
            Forward_Index_Builder builder;
            builder.run(bp, identity, parse_plaintext_content);
            THEN("documents are in check")
            {
                std::vector<std::string> expected_documents{
                    "Doc10", "Doc11", "Doc12", "Doc13", "Doc14"};
                auto documents = load_lines(output_file.string() + ".batch.7.documents");
                REQUIRE(documents == expected_documents);
            }
            THEN("terms are in check")
            {
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
            THEN("term IDs")
            {
                binary_collection coll((output_file.string() + ".batch.7").c_str());
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

void write_batch(
    std::string const& basename,
    std::vector<std::string> const& documents,
    std::vector<std::string> const& terms,
    std::vector<std::vector<uint32_t>> const& collection)
{
    std::string document_file = basename + ".documents";
    std::string term_file = basename + ".terms";
    write_lines(document_file, gsl::make_span(documents));
    write_lines(term_file, gsl::make_span(terms));
    std::ofstream os(basename);
    Forward_Index_Builder::write_header(os, collection.size());
    for (auto const& seq: collection) {
        Forward_Index_Builder::write_document(os, seq.begin(), seq.end());
    }
}

TEST_CASE("Merge forward index batches", "[parsing][forward_index]")
{
    pisa::TemporaryDirectory tmpdir;
    auto dir = tmpdir.path();
    GIVEN("Three batches on disk")
    {
        std::vector<path> batch_paths{dir / "fwd.batch.0", dir / "fwd.batch.1", dir / "fwd.batch.2"};
        write_batch(
            batch_paths[0].string(),
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
        write_batch(
            batch_paths[1].string(),
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
        write_batch(
            batch_paths[2].string(),
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

        WHEN("Merging function is called")
        {
            auto output_file = (dir / "fwd").string();
            Forward_Index_Builder builder;
            builder.merge(output_file, 5, 3);

            THEN("documents are in check")
            {
                std::vector<std::string> expected_documents{
                    "Doc10", "Doc11", "Doc12", "Doc13", "Doc14"};
                auto documents = load_lines(output_file + ".documents");
                REQUIRE(documents == expected_documents);
            }
            THEN("terms are in check")
            {
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
            THEN("term IDs")
            {
                binary_collection coll((output_file).c_str());
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
    auto map_word = [&](std::string&& word) { vec.push_back(word); };
    SECTION("empty")
    {
        parse_html_content(
            "HTTP/1.1 200 OK\n"
            "Content-Length: 16254\n\n"
            "<a/>",
            map_word);
        REQUIRE(vec.empty());
    }
    SECTION("non-empty")
    {
        parse_html_content(
            "HTTP/1.1 200 OK\n"
            "Content-Length: 16254\n\n"
            "<a>lorem</a>ipsum",
            map_word);
        REQUIRE(vec == std::vector<std::string>{"lorem", "ipsum"});
    }
    SECTION("non-empty with CR")
    {
        parse_html_content(
            "HTTP/1.1 200 OK\n"
            "Content-Length: 16254\n\r\n"
            "<a>lorem</a>ipsum",
            map_word);
        REQUIRE(vec == std::vector<std::string>{"lorem", "ipsum"});
    }
}

[[nodiscard]] auto load_term_map(std::string const& basename) -> std::vector<std::string>
{
    std::vector<std::string> map;
    std::ifstream is(basename + ".terms");
    std::string str;
    while (std::getline(is, str)) {
        map.push_back(str);
    }
    return map;
}

TEST_CASE("Build forward index", "[parsing][forward_index][integration]")
{
    auto next_record = [](std::istream& in) -> std::optional<Document_Record> {
        Plaintext_Record record;
        if (in >> record) {
            return Document_Record(record.trecid(), record.content(), record.url());
        }
        return std::nullopt;
    };

    GIVEN("A plaintext collection file")
    {
        std::string input(PISA_SOURCE_DIR "/test/test_data/clueweb1k.plaintext");
        REQUIRE(boost::filesystem::exists(boost::filesystem::path(input)) == true);
        int thread_count = GENERATE(2, 8);
        int batch_size = GENERATE(123, 1000);
        WHEN("Build a forward index")
        {
            pisa::TemporaryDirectory tmpdir;
            auto dir = tmpdir.path();
            std::string output = (dir / "fwd").string();

            std::ifstream is(input);
            Forward_Index_Builder builder;
            builder.build(
                is,
                output,
                next_record,
                [] {
                    return [](std::string&& term) -> std::string {
                        return std::forward<std::string>(term);
                    };
                },
                parse_plaintext_content,
                batch_size,
                thread_count);

            THEN("The collection mapped to terms matches input")
            {
                auto term_map = load_term_map(output);
                auto term_lexicon_buffer = Payload_Vector_Buffer::from_file(output + ".termlex");
                auto term_lexicon = Payload_Vector<std::string>(term_lexicon_buffer);
                REQUIRE(
                    std::vector<std::string>(term_lexicon.begin(), term_lexicon.end()) == term_map);
                binary_collection coll((output).c_str());
                auto seq_iter = coll.begin();
                REQUIRE(*seq_iter->begin() == 1000);
                ++seq_iter;
                std::ifstream plain_is(input);
                std::optional<Document_Record> record = std::nullopt;
                while ((record = next_record(plain_is)).has_value()) {
                    std::vector<std::string> original_body;
                    std::istringstream content_stream(record->content());
                    std::string term;
                    while (content_stream >> term) {
                        EnglishTokenStream tok(term);
                        std::for_each(tok.begin(), tok.end(), [&original_body](auto term) {
                            original_body.push_back(std::move(term));
                        });
                    }
                    std::vector<std::string> produced_body;
                    for (auto term_id: *seq_iter) {
                        produced_body.push_back(term_map[term_id]);
                    }
                    REQUIRE(produced_body == original_body);
                    ++seq_iter;
                }
                auto batch_files = ls(dir, [](auto const& filename) {
                    return filename.find("batch") != std::string::npos;
                });
                REQUIRE(batch_files.empty());
            }
            AND_THEN("Document lexicon contains the same titles as text file")
            {
                auto documents = io::read_string_vector(output + ".documents");
                auto doc_lexicon_buffer = Payload_Vector_Buffer::from_file(output + ".doclex");
                auto doc_lexicon = Payload_Vector<std::string>(doc_lexicon_buffer);
                REQUIRE(std::vector<std::string>(doc_lexicon.begin(), doc_lexicon.end()) == documents);
            }
        }
    }
}
