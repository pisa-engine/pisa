#include <optional>
#include <vector>
#include <string>

#include <fmt/format.h>

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

    and_query<true> and_q;
    std::size_t qid = 0;
    for (auto const &query : queries) {
        auto query_id = query.id.has_value() ? query.id.value() : std::to_string(qid);
        if (query.terms.size() == 1) {
            std::cout << fmt::format("{}\t{}\t{}\n",
                                     query_id,
                                     index[query.terms[0]].size(),
                                     wdata.max_term_weight(query.terms[0]));
        } else {
            auto results = and_q(make_scored_cursors(index, wdata, query), index.num_docs());
            std::sort(results.begin(), results.end(), [](auto const &lhs, auto const &rhs) {
                return lhs.second > rhs.second;
            });
            auto max_score = results.empty() ? 0.0 : results[0].second;
            std::cout << fmt::format("{}\t{}\t{}\n", query_id, results.size(), max_score);
        }
        qid += 1;
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
    std::optional<std::string> stemmer;
    bool compressed = false;

    CLI::App app{"compute_intersection - a tool for pre-computing intersections of terms."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    auto *terms_opt = app.add_option("--terms", terms_file, "Term lexicon");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, parse_query);
    } else {
        io::for_each_line(std::cin, parse_query);
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
