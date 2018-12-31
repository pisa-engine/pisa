#include <iostream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/optional.hpp>

#include "mio/mmap.hpp"

#include "succinct/mapper.hpp"

#include "index_types.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using namespace ds2i;

template <typename IndexType, typename WandType>
void thresholds(const std::string &                   index_filename,
                const boost::optional<std::string> &  wand_data_filename,
                const std::vector<ds2i::term_id_vec> &queries,
                const boost::optional<std::string> &  thresholds_filename,
                std::string const &                   type,
                std::string const &                   query_type,
                uint64_t                              k) {
    using namespace ds2i;
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, query_type, boost::is_any_of(":"));
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

    wand_query<WandType> query_func(wdata, k);
    for (auto const& query : queries) {
        query_func(index, query);
        auto  results   = query_func.topk();
        float threshold = 0.0;
        std::cerr << results.size() << "/" << k << '\n';
        if (results.size() == k) {
            std::cerr << "!!!\n";
            auto min = std::min_element(
                results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
                    return lhs.first < rhs.first;
                });
            threshold = min->first;
        }
        std::cout << threshold << '\n';
    }
}

typedef wand_data<bm25, wand_data_raw<bm25>> wand_raw_index;
typedef wand_data<bm25, wand_data_compressed<bm25, uniform_score_compressor>> wand_uniform_index;

int main(int argc, const char **argv) {
    using namespace ds2i;

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
                                                                    query_type,          \
                                                                    k);                  \
        } else {                                                                         \
            thresholds<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,          \
                                                                wand_data_filename,      \
                                                                queries,                 \
                                                                thresholds_filename,     \
                                                                type,                    \
                                                                query_type,              \
                                                                k);                      \
        }                                                                                \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }
}
