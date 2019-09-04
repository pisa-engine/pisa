#include <optional>
#include <vector>

#include "CLI/CLI.hpp"
#include "index_types.hpp"
#include "pisa/cursor/scored_cursor.hpp"
#include "pisa/query/queries.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void intersect(const std::string &index_filename,
               const std::optional<std::string> &wand_data_filename,
               const std::vector<Query> &queries,
               std::string const &type)
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

    and_query<false> and_q;
    for (auto const &query : queries) {
        auto results = and_q(make_scored_cursors(index, wdata, query), index.num_docs());
        for (auto &&t : query.terms) {
            std::cout << t << " ";
        }
        std::cout << results.size() << std::endl;
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

int main(int argc, const char **argv)
{
    std::string type;
    std::string index_filename;
    std::optional<std::string> terms_file;
    std::optional<std::string> wand_data_filename;
    std::optional<std::string> query_filename;
    bool compressed = false;

    CLI::App app{"compute_intersection - a tool for pre-computing intersections of terms."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto push_query = [&](std::string const &query_line) {
        queries.push_back(parse_query_ids(query_line));
    };

    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, push_query);
    } else {
        io::for_each_line(std::cin, push_query);
    }

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                       \
    }                                                               \
    else if (type == BOOST_PP_STRINGIZE(T))                         \
    {                                                               \
        if (compressed) {                                           \
            intersect<BOOST_PP_CAT(T, _index), wand_uniform_index>( \
                index_filename, wand_data_filename, queries, type); \
        } else {                                                    \
            intersect<BOOST_PP_CAT(T, _index), wand_raw_index>(     \
                index_filename, wand_data_filename, queries, type); \
        }                                                           \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}
