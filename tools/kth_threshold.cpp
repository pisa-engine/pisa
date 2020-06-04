#include <iostream>
#include <optional>
#include <unordered_set>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"
#include <boost/functional/hash.hpp>

#include "mio/mmap.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "app.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "query/algorithm.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

template <typename IndexType, typename WandType>
void kt_thresholds(
    const std::string& index_filename,
    const std::string& wand_data_filename,
    const std::vector<Query>& queries,
    std::string const& type,
    ScorerParams const& scorer_params,
    uint64_t k,
    bool quantized,
    std::optional<std::string> pairs_filename,
    std::optional<std::string> triples_filename)
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    auto scorer = scorer::from_params(scorer_params, wdata);

    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error) {
        spdlog::error("error mapping file: {}, exiting...", error.message());
        std::abort();
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);

    using Pair = std::set<uint32_t>;
    std::unordered_set<Pair, boost::hash<Pair>> pairs_set;

    using Triple = std::set<uint32_t>;
    std::unordered_set<Triple, boost::hash<Triple>> triples_set;

    std::string t;
    if (pairs_filename) {
        std::ifstream pin(*pairs_filename);
        while (std::getline(pin, t)) {
            std::vector<std::string> p;
            boost::algorithm::split(p, t, boost::is_any_of(" \t"));
            pairs_set.insert({(uint32_t)std::stoi(p[0]), (uint32_t)std::stoi(p[1])});
        }
        spdlog::info("Number of pairs loaded: {}", pairs_set.size());
    }

    if (triples_filename) {
        std::ifstream trin(*triples_filename);
        while (std::getline(trin, t)) {
            std::vector<std::string> p;
            boost::algorithm::split(p, t, boost::is_any_of(" \t"));
            triples_set.insert(
                {(uint32_t)std::stoi(p[0]), (uint32_t)std::stoi(p[1]), (uint32_t)std::stoi(p[2])});
        }
        spdlog::info("Number of triples loaded: {}", triples_set.size());
    }

    for (auto const& query: queries) {
        float threshold = 0;

        auto terms = query.terms;
        topk_queue topk(k);
        wand_query wand_q(topk);

        for (size_t i = 0; i < terms.size(); ++i) {
            Query q;
            q.terms.push_back(terms[i]);
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
        for (size_t i = 0; i < terms.size(); ++i) {
            for (size_t j = i + 1; j < terms.size(); ++j) {
                if (pairs_set.count({terms[i], terms[j]})) {
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
                    if (triples_set.count({terms[i], terms[j], terms[s]})) {
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

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string query_filename;
    std::optional<std::string> pairs_filename;
    std::optional<std::string> triples_filename;
    size_t k;
    std::string index_filename;
    std::string wand_data_filename;
    bool quantized = false;

    App<arg::Index, arg::WandData<arg::WandMode::Required>, arg::Query<arg::QueryMode::Ranked>, arg::Scorer>
        app{"A tool for performing threshold estimation using k-th score informations."};

    app.add_option("-p,--pairs", pairs_filename, "Pairs filename");
    app.add_option("-t,--triples", triples_filename, "Triples filename");
    app.add_flag("--quantized", quantized, "Quantizes the scores");

    CLI11_PARSE(app, argc, argv);

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        app.queries(),
        app.index_encoding(),
        app.scorer_params(),
        app.k(),
        quantized,
        pairs_filename,
        triples_filename);

    /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                                                      \
    }                                                                                              \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                        \
    {                                                                                              \
        if (app.is_wand_compressed()) {                                                            \
            if (quantized) {                                                                       \
                std::apply(                                                                        \
                    kt_thresholds<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>, params); \
            } else {                                                                               \
                std::apply(kt_thresholds<BOOST_PP_CAT(T, _index), wand_uniform_index>, params);    \
            }                                                                                      \
        } else {                                                                                   \
            std::apply(kt_thresholds<BOOST_PP_CAT(T, _index), wand_raw_index>, params);            \
        }
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
