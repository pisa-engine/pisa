#include <functional>
#include <iostream>
#include <optional>
#include <thread>

#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>

#include "InvertedIndex.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "mappable/mapper.hpp"
#include "payload_vector.hpp"
#include "query/Query.hpp"
#include "query/QueryProcessor.hpp"
#include "scorer/scorer.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;
using ranges::view::enumerate;

void evaluate_queries(QueryProcessor &processor, gsl::span<Query const> queries)
{
    for (Query const& query: queries) {
        auto results = processor.process(query);
    }
}

template <typename Index, typename Wand, typename Scorer>
void evaluate_queries(
    Index &index,
    Wand &wdata,
    Scorer &scorer,
    uint64_t k,
    std::vector<Query> const &queries,
    std::string const &query_type,
    std::string const &docmap_filename,
    std::string const &run_id = "R0",
    std::string const &iteration = "Q0")
{
    auto processor = make_query_processor(query_type, index, scorer, wdata, k);
    /* evaluate_queries(processor, gsl::span<Query const>(queries)); */

    auto docmap_source = std::make_shared<mio::mmap_source>(docmap_filename.c_str());
    auto docmap = Payload_Vector<>::from(*docmap_source);
    std::vector<std::vector<std::pair<float, uint64_t>>> raw_results(queries.size());
    auto start_batch = std::chrono::steady_clock::now();
    tbb::parallel_for(size_t(0), queries.size(), [&](size_t query_idx) {
        raw_results[query_idx] = processor.process(queries[query_idx]);
    });
    auto end_batch = std::chrono::steady_clock::now();

    for (size_t query_idx = 0; query_idx < raw_results.size(); ++query_idx) {
        auto results = raw_results[query_idx];
        auto qid = queries[query_idx].id;
        for (auto &&[rank, result] : enumerate(results)) {
            std::cout << fmt::format(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
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
}

int main(int argc, const char **argv)
{
    spdlog::set_default_logger(spdlog::stderr_color_mt("default"));

    std::string type;
    std::string query_type;
    std::string index_filename;
    std::optional<std::string> terms_file;
    std::string docmap_filename;
    std::string scorer_name;
    std::optional<std::string> wand_data_filename;
    std::optional<std::string> query_filename;
    std::optional<std::string> thresholds_filename;
    std::optional<std::string> stopwords_filename;
    std::optional<std::string> stemmer = std::nullopt;
    std::string run_id = "R0";
    uint64_t k = configuration::get().k;
    size_t threads = std::thread::hardware_concurrency();
    bool compressed = false;

    CLI::App app{"Retrieves query results in TREC format."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-a,--algorithm", query_type, "Query algorithm")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_option("-r,--run", run_id, "Run identifier");
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_option("--threads", threads, "Thread Count");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("--stopwords", stopwords_filename, "File containing stopwords to ignore");
    app.add_option("-k", k, "k value");
    auto *terms_opt = app.add_option("--terms", terms_file, "Term lexicon");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    app.add_option("--documents", docmap_filename, "Document lexicon")->required();
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    spdlog::info("Number of threads: {}", threads);

    std::vector<Query> queries;
    auto process_term = query::term_processor(terms_file, stemmer);

    std::unordered_set<TermID> stopwords;
    if (stopwords_filename) {
        std::ifstream is(*stopwords_filename);
        io::for_each_line(is, [&](auto &&word) {
            if (auto processed_term = process_term(std::move(word)); process_term) {
                stopwords.insert(*processed_term);
            }
        });
    }

    if (run_id.empty())
        run_id = "R0";

    auto push_query = [&](std::string const &query_line) {
        queries.push_back(query::parse(query_line, process_term, stopwords));
    };

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    } else {
        io::for_each_line(std::cin, push_query);
    }

    auto load_wand_data = [&](auto &wdata, auto &source) {
        std::error_code error;
        source.map(*wand_data_filename, error);
        if (error) {
            spdlog::error("error mapping file: {}, exiting...", error.message());
            std::abort();
        }
        mapper::map(wdata, source, mapper::map_flags::warmup);
    };

    with_index(type, index_filename, [&](auto index) {
        with_wdata(compressed, [&](auto wdata) {
            mio::mmap_source source;
            if (wand_data_filename) {
                load_wand_data(wdata, source);
            }
            with_scorer(scorer_name, wdata, [&](auto scorer) {
                evaluate_queries(index, wdata, scorer, k, queries, query_type, docmap_filename);
            });
        });
    });
}
