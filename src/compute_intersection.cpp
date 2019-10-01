#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "CLI/CLI.hpp"
#include "index_types.hpp"
#include "intersection.hpp"
#include "pisa/cursor/scored_cursor.hpp"
#include "pisa/query/queries.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using pisa::intersection::IntersectionType;
using pisa::intersection::Mask;

template <typename IndexType, typename WandType>
void intersect(std::string const &index_filename,
               std::optional<std::string> const &wand_data_filename,
               std::vector<Query> const &queries,
               std::string const &type,
               IntersectionType intersection_type,
               std::optional<std::uint8_t> max_term_count = std::nullopt)
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

    std::size_t qid = 0u;

    auto print_intersection = [&](auto const &query, auto const &mask) {
        auto intersection = Intersection::compute(index, wdata, query, mask);
        std::cout << fmt::format("{}\t{}\t{}\t{}\n",
                                 query.id ? *query.id : std::to_string(qid),
                                 mask.to_ulong(),
                                 intersection.length,
                                 intersection.max_score);
    };

    for (auto const &query : queries) {
        if (intersection_type == IntersectionType::Combinations) {
            for_all_subsets(query, *max_term_count, print_intersection);
        } else {
            auto intersection = Intersection::compute(index, wdata, query);
            std::cout << fmt::format("{}\t{}\t{}\n",
                                     query.id ? *query.id : std::to_string(qid),
                                     intersection.length,
                                     intersection.max_score);
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
    std::optional<std::uint8_t> max_term_count;
    bool combinations = false;
    bool compressed = false;
    bool header = false;

    CLI::App app{"compute_intersection - a tool for pre-computing intersections of terms."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    auto *terms_opt = app.add_option("--terms", terms_file, "Term lexicon");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    auto *combinations_flag = app.add_flag(
        "--combinations", combinations, "Compute intersections for combinations of terms in query");
    app.add_option("--max-term-count,--mtc",
                   max_term_count,
                   "Max number of terms when computing combinations")
        ->needs(combinations_flag);
    app.add_flag("--header", header, "Write TSV header");
    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    if (query_filename) {
        std::ifstream is(*query_filename);
        io::for_each_line(is, parse_query);
    } else {
        io::for_each_line(std::cin, parse_query);
    }

    if (header) {
        if (combinations) {
            std::cout << "qid\tterm_mask\tlength\tmax_score\n";
        } else {
            std::cout << "qid\tlength\tmax_score\n";
        }
    }

    IntersectionType intersection_type =
        combinations ? IntersectionType::Combinations : IntersectionType::Query;

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                          \
    }                                                                                  \
    else if (type == BOOST_PP_STRINGIZE(T))                                            \
    {                                                                                  \
        if (compressed) {                                                              \
            intersect<BOOST_PP_CAT(T, _index), wand_uniform_index>(index_filename,     \
                                                                   wand_data_filename, \
                                                                   queries,            \
                                                                   type,               \
                                                                   intersection_type,  \
                                                                   max_term_count);    \
        } else {                                                                       \
            intersect<BOOST_PP_CAT(T, _index), wand_raw_index>(index_filename,         \
                                                               wand_data_filename,     \
                                                               queries,                \
                                                               type,                   \
                                                               intersection_type,      \
                                                               max_term_count);        \
        }                                                                              \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }
}
