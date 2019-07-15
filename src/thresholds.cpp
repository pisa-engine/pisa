#include <iostream>
#include <optional>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"

#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "index_types.hpp"
#include "io.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include "cursor/max_scored_cursor.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void thresholds(const std::string &index_filename,
                const std::optional<std::string> &wand_data_filename,
                const std::vector<Query> &queries,
                const std::optional<std::string> &thresholds_filename,
                std::string const &type,
                uint64_t k)
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

    wand_query wand_q(k);
    for (auto const &query : queries) {
        wand_q(make_max_scored_cursors(index, wdata, query), index.num_docs());
        auto  results   = wand_q.topk();
        float threshold = 0.0;
        if (results.size() == k) {
            threshold = results.back().first;
        }
        std::cout << threshold << '\n';
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<uniform_score_compressor>>;

int main(int argc, const char **argv)
{
    std::string type;
    std::string index_filename;
    std::optional<std::string> terms_file;
    std::optional<std::string> wand_data_filename;
    std::optional<std::string> query_filename;
    std::optional<std::string> thresholds_filename;
    std::optional<std::string> stemmer = std::nullopt;

    uint64_t k = configuration::get().k;
    bool compressed = false;

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
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    CLI11_PARSE(app, argc, argv);

    auto process_term = query::term_processor(terms_file, stemmer);

    std::vector<Query> queries;
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
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                            \
    }                                                                                    \
    else if (type == BOOST_PP_STRINGIZE(T)) {                                            \
        if (compressed) {                                                                \
            thresholds<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,      \
                                                                    wand_data_filename,  \
                                                                    queries,             \
                                                                    thresholds_filename, \
                                                                    type,                \
                                                                    k);                  \
        } else {                                                                         \
            thresholds<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,          \
                                                                wand_data_filename,      \
                                                                queries,                 \
                                                                thresholds_filename,     \
                                                                type,                    \
                                                                k);                      \
        }                                                                                \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}
