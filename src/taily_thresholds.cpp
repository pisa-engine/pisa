#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/kruskal_min_spanning_tree.hpp>
#include <iostream>
#include <optional>

#include "mio/mmap.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "io.hpp"
#include "query/queries.hpp"
#include "taily.hpp"
#include "util/util.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;
using Graph = boost::adjacency_list<boost::vecS,
                                    boost::vecS,
                                    boost::undirectedS,
                                    boost::no_property,
                                    boost::property<boost::edge_weight_t, float>>;
using Edge = boost::graph_traits<Graph>::edge_descriptor;

[[nodiscard]] auto estimate_pairwise_cutoff(taily::Query_Statistics const &stats,
                                            int ntop,
                                            double all) -> double
{
    auto const dist = taily::fit_distribution(stats.term_stats);
    double const p_c = std::min(1.0, ntop / all);
    return boost::math::quantile(complement(dist, p_c));
}

void pairwise_thresholds(const std::string &taily_stats_filename,
                         const std::map<std::set<uint32_t>, float> &bigrams_stats,
                         const std::vector<Query> &queries,
                         uint64_t k)
{
    std::ifstream ifs(taily_stats_filename);
    int64_t collection_size;
    ifs.read(reinterpret_cast<char *>(&collection_size), sizeof(collection_size));
    int64_t term_num;
    ifs.read(reinterpret_cast<char *>(&term_num), sizeof(term_num));

    std::vector<taily::Feature_Statistics> stats;
    for (int i = 0; i < term_num; ++i) {
        stats.push_back(taily::Feature_Statistics::from_stream(ifs));
    }

    for (auto const &query : queries) {
        int num_nodes = query.terms.size();
        std::vector<std::pair<int, int>> edge_array;
        std::vector<float> weights;
        for (size_t i = 0; i < query.terms.size(); ++i) {
            for (size_t j = i + 1; j < query.terms.size(); ++j) {
                auto it = bigrams_stats.find({query.terms[i], query.terms[j]});
                if (it != bigrams_stats.end()) {
                    edge_array.emplace_back(i, j);
                    double t1 = stats[i].frequency /collection_size;
                    double t2 = stats[j].frequency /collection_size;
                    double t12 = it->second /collection_size;
                    weights.push_back(-t12/(t1 * t2));
                }
            }
        }
        Graph g(edge_array.begin(), edge_array.end(), weights.begin(), num_nodes);
        boost::property_map<Graph, boost::edge_weight_t>::type weight = get(boost::edge_weight, g);
        std::vector<Edge> spanning_tree;
        boost::kruskal_minimum_spanning_tree(g, std::back_inserter(spanning_tree));

        double all = 1;

        std::vector<taily::Feature_Statistics> term_stats;
        auto terms = query.terms;
        for (auto &&t : terms) {
            term_stats.push_back(stats[t]);
        }
        taily::Query_Statistics query_stats{term_stats, collection_size};
        double const any = taily::any(query_stats);

        for (std::vector<Edge>::iterator ei = spanning_tree.begin(); ei != spanning_tree.end();
             ++ei) {
            all *= -(weight[*ei]/ any);
        }

        for (auto &&t : terms) {
            all *= stats[t].frequency / any;
        }
        all *= any;

        auto threshold = estimate_pairwise_cutoff(query_stats, k, all);
        std::cout << threshold << '\n';
    }
}

void thresholds(const std::string &taily_stats_filename,
                const std::vector<Query> &queries,
                uint64_t k)
{
    std::ifstream ifs(taily_stats_filename);
    int64_t collection_size;
    ifs.read(reinterpret_cast<char *>(&collection_size), sizeof(collection_size));
    int64_t term_num;
    ifs.read(reinterpret_cast<char *>(&term_num), sizeof(term_num));

    std::vector<taily::Feature_Statistics> stats;
    for (int i = 0; i < term_num; ++i) {
        stats.push_back(taily::Feature_Statistics::from_stream(ifs));
    }

    for (auto const &query : queries) {
        std::vector<taily::Feature_Statistics> term_stats;
        auto terms = query.terms;
        for (auto &&t : terms) {
            term_stats.push_back(stats[t]);
        }
        taily::Query_Statistics query_stats{term_stats, collection_size};
        auto threshold = taily::estimate_cutoff(query_stats, k);
        std::cout << threshold << '\n';
    }
}

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string taily_stats_filename;
    std::optional<std::string> terms_file;
    std::optional<std::string> query_filename;
    std::optional<std::string> stemmer = std::nullopt;
    uint64_t k = configuration::get().k;

    std::optional<std::string> pairwise_filename;

    CLI::App app{"A tool for predicting thresholds for queries using Taily."};
    app.set_config("--config", "", "Configuration .ini file", false);
    app.add_option("-t,--taily", taily_stats_filename, "Taily stats filename")->required();
    app.add_option("-q,--query", query_filename, "Queries filename");
    app.add_option("-k", k, "k value");
    app.add_option("-p, --pairwise", pairwise_filename, "Pairwise filename");
    auto *terms_opt =
        app.add_option("--terms", terms_file, "Text file with terms in separate lines");
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

    if (pairwise_filename) {
        std::map<std::set<uint32_t>, float> bigrams_stats;
        std::ifstream bigrams_fs(*pairwise_filename);
        auto term_processor = TermProcessor(terms_file, std::nullopt, stemmer);
        std::string line;
        while (std::getline(bigrams_fs, line)) {
            bigrams_stats.insert({{0, 0}, 0.0});
        }

        pairwise_thresholds(taily_stats_filename, bigrams_stats, queries, k);
    } else {
        thresholds(taily_stats_filename, queries, k);
    }
}
