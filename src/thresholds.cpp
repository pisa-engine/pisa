#include <iostream>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/optional.hpp"

#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "succinct/mapper.hpp"

#include "index_types.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void thresholds(const std::string &                   index_filename,
                const boost::optional<std::string> &  wand_data_filename,
                const std::vector<term_id_vec> &queries,
                const boost::optional<std::string> &  thresholds_filename,
                std::string const &                   type,
                uint64_t                              k)
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    mio::mmap_source md;
    if (wand_data_filename) {
        std::error_code error;
        md.map(wand_data_filename.value(), error);
        if(error){
            std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
            throw std::runtime_error("Error opening file");
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    wand_query<IndexType, WandType> query_func(index, wdata, k);
    for (auto const &query : queries) {
        query_func(query);
        auto  results   = query_func.topk();
        float threshold = 0.0;
        if (results.size() == k) {
            threshold = results.back().first;
        }
        std::cout << threshold << '\n';
    }
}

using wand_raw_index = wand_data<bm25, wand_data_raw<bm25>>;
using wand_uniform_index = wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>>;

int main(int argc, const char **argv)
{
    std::string type;
    std::string query_type;
    std::string index_filename;
    boost::optional<std::string> wand_data_filename;
    boost::optional<std::string> query_filename;
    boost::optional<std::string> thresholds_filename;
    uint64_t k = configuration::get().k;
    bool compressed = false;

    CLI::App app{"queries - a tool for performing queries on an index."};
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    CLI11_PARSE(app, argc, argv);

    std::vector<term_id_vec> queries;
    term_id_vec q;
    if (query_filename) {
        std::filebuf fb;
        if (fb.open(query_filename.value(), std::ios::in)) {
            std::istream is(&fb);
            while (read_query(q, is))
                queries.push_back(q);
        }
    } else {
        while (read_query(q))
            queries.push_back(q);
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

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}
