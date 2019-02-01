#include <iostream>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/optional.hpp"

#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "succinct/mapper.hpp"

#include "index_types.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;
using ranges::view::enumerate;

template <typename IndexType, typename WandType>
void evaluate_queries(const std::string &index_filename,
                      const boost::optional<std::string> &wand_data_filename,
                      const std::vector<Query> &queries,
                      const boost::optional<std::string> &thresholds_filename,
                      std::string const &type,
                      uint64_t k,
                      std::string const &documents_filename,
                      std::string const &iteration = "Q0",
                      std::string const &run_id = "R0")
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    mio::mmap_source md;
    if (wand_data_filename) {
        std::error_code error;
        md.map(*wand_data_filename, error);
        if (error) {
            spdlog::error("error mapping file: {}, exiting...", error.message());
            std::abort();
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    auto docmap = io::read_string_vector(documents_filename);
    wand_query<IndexType, WandType> query_func(index, wdata, k);
    for (auto const &[qid, query] : enumerate(queries)) {
        query_func(query.terms);
        auto results = query_func.topk();
        for (auto &&[rank, result] : enumerate(results)) {
            std::cout << fmt::format("{}\t{}\t{}\t{}\t{}\t{}\n",
                                     query.id.value_or(std::to_string(qid)),
                                     iteration,
                                     docmap.at(result.second),
                                     rank,
                                     result.first,
                                     run_id);
        }
    }
}

using wand_raw_index = wand_data<bm25, wand_data_raw<bm25>>;
using wand_uniform_index = wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>>;

int main(int argc, const char **argv)
{
    std::string type;
    std::string query_type;
    std::string index_filename;
    std::optional<std::string> terms_file;
    std::string documents_file;
    boost::optional<std::string> wand_data_filename;
    boost::optional<std::string> query_filename;
    boost::optional<std::string> thresholds_filename;
    uint64_t k = configuration::get().k;
    bool compressed = false;
    bool nostem = false;

    CLI::App app{"queries - a tool for performing queries on an index."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    auto *terms_opt =
        app.add_option("--terms", terms_file, "Text file with terms in separate lines");
    app.add_flag("--nostem", nostem, "Do not stem terms")->needs(terms_opt);
    app.add_option("--documents", documents_file, "Text file with documents in separate lines")
        ->required();
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto process_term = query::term_processor(terms_file, not nostem);
    auto push_query = [&](std::string const &query_line) {
        queries.push_back(parse_query(query_line, process_term));
    };

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    }
    else {
        io::for_each_line(std::cin, push_query);
    }

    /**/
    if (false) { // NOLINT
#define LOOP_BODY(R, DATA, T)                                                                      \
    }                                                                                              \
    else if (type == BOOST_PP_STRINGIZE(T))                                                        \
    {                                                                                              \
        if (compressed) {                                                                          \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,          \
                                                                          wand_data_filename,      \
                                                                          queries,                 \
                                                                          thresholds_filename,     \
                                                                          type,                    \
                                                                          k,                       \
                                                                          documents_file);         \
        }                                                                                          \
        else {                                                                                     \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,              \
                                                                      wand_data_filename,          \
                                                                      queries,                     \
                                                                      thresholds_filename,         \
                                                                      type,                        \
                                                                      k,                           \
                                                                      documents_file);             \
        }                                                                                          \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
    }
    else {
        spdlog::error("Unknown type {}", type);
    }
}
