#include <iostream>
#include <optional>
#include <unordered_set>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include <boost/functional/hash.hpp>
#include "boost/algorithm/string/split.hpp"

#include "mio/mmap.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "query/algorithm.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "scorer/scorer.hpp"
#include "query/algorithm.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));


    std::string query_filename;
    std::string scores_filename;
    std::optional<std::string> pairs_filename;
    std::optional<std::string> triples_filename;

    std::optional<std::string> terms_file;
    std::optional<std::string> stemmer = std::nullopt;

    CLI::App app{"A tool for performing threshold estimation using k-th term score informations."};
    app.add_option("-q,--query", query_filename, "Queries filename")->required();
    app.add_option("-s,--scores", scores_filename, "Queries filename")->required();
    auto *terms_opt =
        app.add_option("--terms", terms_file, "Text file with terms in separate lines");
    app.add_option("--stemmer", stemmer, "Stemmer type")->needs(terms_opt);
    app.add_option("-p,--pairs", pairs_filename, "Pairs filename");
    app.add_option("-t,--triples", triples_filename, "Triples filename");

    CLI11_PARSE(app, argc, argv);

    std::vector<Query> queries;
    auto parse_query = resolve_query_parser(queries, terms_file, std::nullopt, stemmer);
    std::ifstream is(query_filename);
    io::for_each_line(is, parse_query);

    std::vector<float> scores;
    std::ifstream tin(scores_filename);
    std::string t;
    while (std::getline(tin, t)) {
        scores.push_back(std::stof(t));
    }

    using Pair = std::set<uint32_t>;
    std::unordered_set<Pair, boost::hash<Pair>> pairs_set;

    using Triple = std::set<uint32_t>;
    std::unordered_set<Triple, boost::hash<Triple>> triples_set;


    std::string index_filename = "/home/amallia/collections/CW09B/CW09B.url.block_simdbp";
    block_simdbp_index index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    using wand_raw_index = wand_data<wand_data_raw>;
    std::string wand_data_filename = "/home/amallia/collections/CW09B/CW09B.url.bm25.bmw";
    wand_raw_index wdata;
    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error) {
        spdlog::error("error mapping file: {}, exiting...", error.message());
        std::abort();
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);


    auto scorer = scorer::from_name("bm25", wdata);

    if(pairs_filename){
        std::ifstream pin(*pairs_filename);
        while (std::getline(pin, t)) {
            std::vector<std::string> p;
            boost::algorithm::split(p, t, boost::is_any_of(" \t"));
            pairs_set.insert({(uint32_t)std::stoi(p[0]), (uint32_t)std::stoi(p[1])});
        }
        spdlog::info("Number of pairs loaded: {}", pairs_set.size());
    }

    if(triples_filename){
        std::ifstream trin(*triples_filename);
        while (std::getline(trin, t)) {
            std::vector<std::string> p;
            boost::algorithm::split(p, t, boost::is_any_of(" \t"));
            triples_set.insert({(uint32_t)std::stoi(p[0]), (uint32_t)std::stoi(p[1]), (uint32_t)std::stoi(p[2])});
        }
        spdlog::info("Number of triples loaded: {}", triples_set.size());
    }
    for (auto const &query : queries) {
        float threshold = 0;
        for (auto &&t : query.terms){
            threshold = std::max(threshold, scores[t]);


        }

        auto terms = query.terms;
        auto k = 1000;
        topk_queue topk(k);
        wand_query wand_q(topk);
        for (size_t i = 0; i < terms.size(); ++i) {
            for (size_t j = i + 1; j < terms.size(); ++j) {
                if(pairs_set.count({terms[i], terms[j]})){
                    Query q;
                    q.terms.push_back(terms[i]);
                    q.terms.push_back(terms[j]);
                    wand_q(make_max_scored_cursors(index, wdata, *scorer, q), index.num_docs());
                    topk.finalize();
                    auto results = topk.topk();
                    topk.clear();
                    float t = 0.0;
                    if (results.size() == k) {
                        t = results.back().first;

                    }
                    threshold = std::max(threshold, t);
                }
            }
        }
        for (size_t i = 0; i < terms.size(); ++i) {
            for (size_t j = i + 1; j < terms.size(); ++j) {
                for (size_t s = j + 1; s < terms.size(); ++s) {

                    if(triples_set.count({terms[i], terms[j], terms[s]})){
                        Query q;
                        q.terms.push_back(terms[i]);
                        q.terms.push_back(terms[j]);
                        q.terms.push_back(terms[s]);
                        wand_q(make_max_scored_cursors(index, wdata, *scorer, q), index.num_docs());
                        topk.finalize();
                        auto results = topk.topk();
                        topk.clear();
                        float t = 0.0;
                        if (results.size() == k) {
                            t = results.back().first;

                        }
                        threshold = std::max(threshold, t);
                    }
                }
            }
        }
        std::cout << threshold << '\n';
    }
}
