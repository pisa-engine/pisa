#include <fstream>
#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "topk_queue.hpp"
#include "v1/analyze_query.hpp"
#include "v1/blocked_cursor.hpp"
#include "v1/daat_or.hpp"
#include "v1/index_metadata.hpp"
#include "v1/intersection.hpp"
#include "v1/maxscore.hpp"
#include "v1/query.hpp"
#include "v1/raw_cursor.hpp"
#include "v1/scorer/bm25.hpp"
#include "v1/scorer/runner.hpp"
#include "v1/types.hpp"
#include "v1/union_lookup.hpp"

using pisa::resolve_query_parser;
using pisa::TermProcessor;
using pisa::v1::BlockedReader;
using pisa::v1::daat_or;
using pisa::v1::DaatOrAnalyzer;
using pisa::v1::index_runner;
using pisa::v1::IndexMetadata;
using pisa::v1::ListSelection;
using pisa::v1::maxscore_union_lookup;
using pisa::v1::MaxscoreAnalyzer;
using pisa::v1::MaxscoreUnionLookupAnalyzer;
using pisa::v1::Query;
using pisa::v1::QueryAnalyzer;
using pisa::v1::RawReader;
using pisa::v1::resolve_yml;
using pisa::v1::unigram_union_lookup;
using pisa::v1::UnigramUnionLookupAnalyzer;
using pisa::v1::union_lookup;
using pisa::v1::UnionLookupAnalyzer;
using pisa::v1::VoidScorer;

int main(int argc, char** argv)
{
    pisa::QueryApp app("Filters out empty queries against a v1 index.");
    CLI11_PARSE(app, argc, argv);

    auto meta = IndexMetadata::from_file(resolve_yml(app.yml));
    auto stemmer = meta.stemmer ? std::make_optional(*meta.stemmer) : std::optional<std::string>{};
    if (meta.term_lexicon) {
        app.terms_file = meta.term_lexicon.value();
    }

    auto term_processor = TermProcessor(app.terms_file, {}, stemmer);
    auto filter = [&](auto&& line) {
        auto query = parse_query_terms(line, term_processor);
        if (not query.terms.empty()) {
            std::cout << line << '\n';
        }
    };
    if (app.query_file) {
        std::ifstream is(*app.query_file);
        pisa::io::for_each_line(is, filter);
    } else {
        pisa::io::for_each_line(std::cin, filter);
    }
    return 0;
}
