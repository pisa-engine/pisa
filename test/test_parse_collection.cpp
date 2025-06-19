#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <vector>

#include <fmt/format.h>
#include <fstream>
#include <memory>

#include "binary_collection.hpp"
#include "fmt/core.h"
#include "forward_index_builder.hpp"
#include "io.hpp"
#include "parser.hpp"
#include "payload_vector.hpp"
#include "pisa_config.hpp"
#include "temporary_directory.hpp"
#include "text_analyzer.hpp"
#include "token_filter.hpp"
#include "tokenizer.hpp"

void build_index(
    pisa::TemporaryDirectory const& tmp,
    std::string const& input_path,
    std::string const& parser_type,
    std::shared_ptr<pisa::TextAnalyzer> analyzer
) {
    auto fwd_base_path = (tmp.path() / "tiny.fwd");
    {
        std::ifstream is(input_path);
        pisa::Forward_Index_Builder builder;
        analyzer->emplace_token_filter<pisa::LowercaseFilter>();
        builder.build(
            is, fwd_base_path.string(), pisa::record_parser(parser_type, is), analyzer, 10, 2
        );
    }
}

auto transform_terms(pisa::binary_collection const& fwd, pisa::Payload_Vector<> termlex)
    -> std::vector<std::vector<std::string>> {
    std::vector<std::vector<std::string>> terms;
    // notice we are skipping the first sequence that encodes document count
    std::transform(
        std::next(fwd.begin()), fwd.end(), std::back_inserter(terms), [&termlex](auto term_ids) {
            std::vector<std::string> doc_terms;
            std::transform(
                term_ids.begin(),
                term_ids.end(),
                std::back_inserter(doc_terms),
                [&termlex](auto term_id) { return std::string(termlex[term_id]); }
            );
            return doc_terms;
        }
    );
    return terms;
}

auto read_collection_from_plaintext(std::string const& path, std::shared_ptr<pisa::TextAnalyzer> analyzer)
    -> std::vector<std::vector<std::string>> {
    auto lines = pisa::io::read_string_vector(path);
    std::vector<std::vector<std::string>> collection;
    std::transform(
        lines.begin(), lines.end(), std::back_inserter(collection), [&analyzer](auto const& line) {
            auto token_stream = analyzer->analyze(line);
            // note we are skipping title
            return std::vector<std::string>(std::next(token_stream->begin()), token_stream->end());
        }
    );
    return collection;
}

auto verify_output(pisa::TemporaryDirectory const& tmp, std::shared_ptr<pisa::TextAnalyzer> analyzer) {
    auto expected_titles =
        pisa::io::read_string_vector(PISA_SOURCE_DIR "/test/test_data/tiny/tiny.fwd.documents");
    REQUIRE(pisa::io::read_string_vector(tmp.path() / "tiny.fwd.documents") == expected_titles);

    auto doclex_bytes = pisa::io::load_data(tmp.path() / "tiny.fwd.doclex");
    auto doclex = pisa::Payload_Vector<>::from(doclex_bytes);

    std::vector<std::string> titles;
    std::transform(doclex.begin(), doclex.end(), std::back_inserter(titles), [](auto term) {
        return std::string(term);
    });
    REQUIRE(titles == expected_titles);

    auto expected_terms =
        pisa::io::read_string_vector(PISA_SOURCE_DIR "/test/test_data/tiny/tiny.fwd.terms");
    REQUIRE(pisa::io::read_string_vector(tmp.path() / "tiny.fwd.terms") == expected_terms);

    auto termlex_bytes = pisa::io::load_data(tmp.path() / "tiny.fwd.termlex");
    auto termlex = pisa::Payload_Vector<>::from(termlex_bytes);

    std::vector<std::string> terms;
    std::transform(termlex.begin(), termlex.end(), std::back_inserter(terms), [](auto term) {
        return std::string(term);
    });
    REQUIRE(terms == expected_terms);

    auto collection = read_collection_from_plaintext(
        PISA_SOURCE_DIR "/test/test_data/tiny/tiny.plaintext", analyzer
    );

    pisa::binary_collection fwd((tmp.path() / "tiny.fwd").c_str());
    auto fwd_terms = transform_terms(fwd, termlex);
    REQUIRE(*fwd.begin()->begin() == 5);
    for (int doc = 0; doc < *fwd.begin()->begin(); ++doc) {
        REQUIRE(fwd_terms[doc] == collection[doc]);
    }
}

TEST_CASE("Parse plaintext collection", "[index][parse]") {
    pisa::TemporaryDirectory tmp;
    auto analyzer = std::make_shared<pisa::TextAnalyzer>(std::make_unique<pisa::EnglishTokenizer>());
    build_index(tmp, PISA_SOURCE_DIR "/test/test_data/tiny/tiny.plaintext", "plaintext", analyzer);
    verify_output(tmp, analyzer);
}

TEST_CASE("Parse JSON collection", "[index][parse]") {
    pisa::TemporaryDirectory tmp;
    auto analyzer = std::make_shared<pisa::TextAnalyzer>(std::make_unique<pisa::EnglishTokenizer>());
    build_index(tmp, PISA_SOURCE_DIR "/test/test_data/tiny/tiny-no-url.jsonl", "jsonl", analyzer);
    verify_output(tmp, analyzer);
}

TEST_CASE("Parse JSON collection with URLs", "[index][parse]") {
    pisa::TemporaryDirectory tmp;
    auto analyzer = std::make_shared<pisa::TextAnalyzer>(std::make_unique<pisa::EnglishTokenizer>());
    build_index(tmp, PISA_SOURCE_DIR "/test/test_data/tiny/tiny-with-url.jsonl", "jsonl", analyzer);
    verify_output(tmp, analyzer);

    auto expected_urls =
        pisa::io::read_string_vector(PISA_SOURCE_DIR "/test/test_data/tiny/tiny.fwd.documents");
    std::transform(
        expected_urls.begin(), expected_urls.end(), expected_urls.begin(), [](auto const& title) {
            return fmt::format("https://{}.net", title);
        }
    );
    REQUIRE(pisa::io::read_string_vector(tmp.path() / "tiny.fwd.urls") == expected_urls);
}
