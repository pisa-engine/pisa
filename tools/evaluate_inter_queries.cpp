#include <algorithm>
#include <string>
/* #include <optional> */
/* #include <thread> */

#include <CLI/CLI.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <mio/mmap.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

/* #include "accumulator/lazy_accumulator.hpp" */
/* #include "cursor/block_max_scored_cursor.hpp" */
/* #include "cursor/max_scored_cursor.hpp" */
#include "cursor/scored_cursor.hpp"
/* #include "index_types.hpp" */
/* #include "io.hpp" */
/* #include "mappable/mapper.hpp" */
#include "query/algorithm/and_query.hpp"
#include "query/algorithm/inter_query.hpp"
#include "query/queries.hpp"
/* #include "scorer/scorer.hpp" */
/* #include "util/util.hpp" */
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::view::enumerate;

template <typename Index, typename Wand, typename Scorer>
using QueryLoop = std::function<std::vector<ResultVector>(
    Index const &, Wand const &, Scorer, std::vector<Query> const &, int)>;

template <typename Index, typename Algorithm, typename Wand, typename Scorer>
auto query_loop(Index const &,
                Wand const &,
                Scorer,
                std::vector<Query> const &,
                int k,
                std::vector<std::vector<std::bitset<64>>> const &intersections)
    -> std::vector<ResultVector>;

#define PISA_RANKED_OR_QUERY_LOOP(SCORER, INDEX, WAND)                  \
    template <>                                                         \
    auto query_loop<BOOST_PP_CAT(INDEX, _index),                        \
                    pisa::IntersectionQuery,                            \
                    wand_data<WAND>,                                    \
                    SCORER<wand_data<WAND>>>(                           \
        BOOST_PP_CAT(INDEX, _index) const &index,                       \
        wand_data<WAND> const &,                                        \
        SCORER<wand_data<WAND>> scorer,                                 \
        std::vector<Query> const &queries,                              \
        int k,                                                          \
        std::vector<std::vector<std::bitset<64>>> const &intersections) \
        ->std::vector<ResultVector>                                     \
    {                                                                   \
        std::vector<ResultVector> results(queries.size());              \
        auto run = IntersectionQuery(k);                                \
        for (std::size_t qidx = 0; qidx < queries.size(); ++qidx) {     \
            auto query = queries[qidx];                                 \
            for (auto instersection : intersections[qidx]) {            \
            }                                                           \
            auto cursors = make_scored_cursors(index, scorer, query);   \
            /*run(gsl::make_span(cursors), index.num_docs());*/         \
            results[qidx] = run.topk();                                 \
        }                                                               \
        return results;                                                 \
    }

#define LOOP_BODY(R, DATA, T)                                \
    PISA_RANKED_OR_QUERY_LOOP(bm25, T, wand_data_raw)        \
    PISA_RANKED_OR_QUERY_LOOP(dph, T, wand_data_raw)         \
    PISA_RANKED_OR_QUERY_LOOP(pl2, T, wand_data_raw)         \
    PISA_RANKED_OR_QUERY_LOOP(qld, T, wand_data_raw)         \
    PISA_RANKED_OR_QUERY_LOOP(bm25, T, wand_data_compressed) \
    PISA_RANKED_OR_QUERY_LOOP(dph, T, wand_data_compressed)  \
    PISA_RANKED_OR_QUERY_LOOP(pl2, T, wand_data_compressed)  \
    PISA_RANKED_OR_QUERY_LOOP(qld, T, wand_data_compressed)  \
/**/
BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
#undef PISA_RANKED_OR_QUERY_LOOP

template <typename IndexType, typename WandType>
void evaluate_queries(const std::string &index_filename,
                      std::string &wand_data_filename,
                      std::vector<Query> const &queries,
                      std::vector<std::vector<std::bitset<64>>> const &intersections,
                      const std::optional<std::string> &thresholds_filename,
                      std::string const &type,
                      std::string const &query_type,
                      uint64_t k,
                      std::string const &documents_filename,
                      std::string const &scorer_name,
                      std::string const &run_id = "R0",
                      std::string const &iteration = "Q0")
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error) {
        spdlog::error("error mapping file: {}, exiting...", error.message());
        std::abort();
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);

    auto run_evaluation = [&](auto scorer) {
        auto source = std::make_shared<mio::mmap_source>(documents_filename.c_str());
        auto docmap = Payload_Vector<>::from(*source);
        auto start_batch = std::chrono::steady_clock::now();
        std::vector<ResultVector> raw_results =
            query_loop(index, wdata, scorer, queries, k, intersections);
        auto end_batch = std::chrono::steady_clock::now();
        for (size_t query_idx = 0; query_idx < raw_results.size(); ++query_idx) {
            auto results = raw_results[query_idx];
            auto qid = queries[query_idx].id;
            for (auto &&[rank, result] : enumerate(results)) {
                std::cout << fmt::format("{}\t{}\t{}\t{}\t{}\t{}\n",
                                         qid.value_or(std::to_string(query_idx)),
                                         iteration,
                                         docmap[result.second],
                                         rank,
                                         result.first,
                                         run_id);
            }
        }
        auto end_print = std::chrono::steady_clock::now();
        double batch_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_batch - start_batch).count();
        double batch_with_print_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_print - start_batch).count();
        spdlog::info("Time taken to process queries: {}ms", batch_ms);
        spdlog::info("Time taken to process queries with printing: {}ms", batch_with_print_ms);
    };
    with_scorer(scorer_name, wdata, run_evaluation);
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

int main(int argc, const char **argv)
{
    spdlog::set_default_logger(spdlog::stderr_color_mt("default"));

    std::string type;
    std::string query_type;
    std::string index_filename;
    std::optional<std::string> terms_file;
    std::string documents_file;
    std::string scorer_name;
    std::string wand_data_filename;
    std::optional<std::string> query_filename;
    std::optional<std::string> thresholds_filename;
    std::optional<std::string> stopwords_filename;
    std::optional<std::string> stemmer = std::nullopt;
    std::string run_id = "R0";
    std::uint64_t k = 1'000;
    bool compressed = false;
    std::string inter_filename;

    CLI::App app{"Retrieves query results in TREC format."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-a,--algorithm", query_type, "Query algorithm")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_option("--intersections", inter_filename, "Intersections filename")->required();
    app.add_option("-r,--run", run_id, "Run identifier");
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    auto *terms_opt = app.add_option("--terms", terms_file, "Term lexicon");
    app.add_option("--stopwords", stopwords_filename, "File containing stopwords to ignore")
        ->needs(terms_opt);
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    app.add_option("--documents", documents_file, "Document lexicon")->required();
    CLI11_PARSE(app, argc, argv);

    if (run_id.empty()) {
        run_id = "R0";
    }

    std::vector<Query> queries;
    auto push_query = resolve_query_parser(queries, terms_file, stopwords_filename, stemmer);

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    } else {
        io::for_each_line(std::cin, push_query);
    }

    auto intersections = [&]() {
        std::vector<std::vector<std::bitset<64>>> intersections;
        std::ifstream is(*query_filename);
        io::for_each_line(is, [&](auto const &query_line) {
            intersections.emplace_back();
            std::istringstream iss(query_line);
            std::transform(std::istream_iterator<std::string>(iss),
                           std::istream_iterator<std::string>(),
                           std::back_inserter(intersections.back()),
                           [&](auto const &n) { return std::bitset<64>(std::stoul(n)); });
        });
        return intersections;
    }();

    /**/
    if (false) { // NOLINT
#define LOOP_BODY(R, DATA, T)                                                                  \
    }                                                                                          \
    else if (type == BOOST_PP_STRINGIZE(T))                                                    \
    {                                                                                          \
        if (compressed) {                                                                      \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,      \
                                                                          wand_data_filename,  \
                                                                          queries,             \
                                                                          intersections,       \
                                                                          thresholds_filename, \
                                                                          type,                \
                                                                          query_type,          \
                                                                          k,                   \
                                                                          documents_file,      \
                                                                          scorer_name,         \
                                                                          run_id);             \
        } else {                                                                               \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,          \
                                                                      wand_data_filename,      \
                                                                      queries,                 \
                                                                      intersections,           \
                                                                      thresholds_filename,     \
                                                                      type,                    \
                                                                      query_type,              \
                                                                      k,                       \
                                                                      documents_file,          \
                                                                      scorer_name,             \
                                                                      run_id);                 \
        }                                                                                      \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", type);
    }
}
