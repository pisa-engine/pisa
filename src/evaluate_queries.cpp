#include <iostream>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/optional.hpp"

#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "cli.hpp"
#include "cursor/max_scored_cursor.hpp"
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
                      const std::optional<std::string> &wand_data_filename,
                      const std::vector<Query> &queries,
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
    wand_query wand_q(k);
    for (auto const &[qid, query] : enumerate(queries)) {
        wand_q(make_max_scored_cursors(index, wdata, query.terms), index.num_docs());
        auto results = wand_q.topk();
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
    CLI::App app{"queries - a tool for performing queries on an index."};
    app.set_config("--config", "", "Configuration .ini file", false);
    auto type = options::index_type(app);
    auto index_basename = options::index_basename(app).with(required);
    auto wand_data_filename = options::wand_data(app).with(required);
    auto compressed = options::wand_compressed(app);
    auto k = options::k(app);
    auto query_filename = option<std::string>(app, "-q,--query", "Query filename");
    auto terms_file = option<std::string>(app, "--terms", "Text file with terms in separate lines");
    auto documents_file =
        option<std::string>(app, "--documents", "Text file with documents in separate lines")
            .with(required);
    auto nostem = flag(app, "--nostem", "Do not stem terms").with(needs(terms_file));
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto process_term = query::term_processor(*terms_file, not nostem);
    auto push_query = [&](std::string const &query_line) {
        queries.push_back(parse_query(query_line, process_term));
    };

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    } else {
        io::for_each_line(std::cin, push_query);
    }

    /**/
    if (false) { // NOLINT
#define LOOP_BODY(R, DATA, T)                                                               \
    }                                                                                       \
    else if (*type == BOOST_PP_STRINGIZE(T))                                                \
    {                                                                                       \
        if (compressed) {                                                                   \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index>(                  \
                *index_basename, *wand_data_filename, queries, *type, *k, *documents_file); \
        } else {                                                                            \
            evaluate_queries<BOOST_PP_CAT(T, _index), wand_raw_index>(                      \
                *index_basename, *wand_data_filename, queries, *type, *k, *documents_file); \
        }                                                                                   \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    }
    else {
        spdlog::error("Unknown type {}", *type);
    }
}
