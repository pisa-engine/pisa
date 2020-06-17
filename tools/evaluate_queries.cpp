#include <iostream>
#include <optional>
#include <thread>

#include <CLI/CLI.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <functional>
#include <mappable/mapper.hpp>
#include <mio/mmap.hpp>
#include <nlohmann/json.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>

#include "accumulator/lazy_accumulator.hpp"
#include "app.hpp"
#include "binary_index.hpp"
#include "cursor/block_max_scored_cursor.hpp"
#include "cursor/inspecting_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "index_types.hpp"
#include "io.hpp"
#include "query.hpp"
#include "query/algorithm.hpp"
#include "scorer/scorer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

using namespace pisa;
using ranges::views::enumerate;

template <typename IndexType, typename WandType>
void evaluate_queries(
    const std::string& index_filename,
    const std::string& wand_data_filename,
    const std::vector<QueryContainer>& queries,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    std::string const& documents_filename,
    ScorerParams const& scorer_params,
    std::string const& run_id,
    std::string const& iteration,
    bool use_thresholds,
    std::optional<std::string>& pair_index_path)
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

    using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;
    auto pair_index = [&]() -> std::optional<pair_index_type> {
        if (pair_index_path) {
            return pair_index_type::load(*pair_index_path);
        }
        return std::nullopt;
    }();

    std::function<std::vector<std::pair<float, uint64_t>>(QueryRequest)> query_fun;

    if (query_type == "wand") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            wand_query wand_q(topk);
            wand_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "block_max_wand") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            block_max_wand_query block_max_wand_q(topk);
            block_max_wand_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "block_max_maxscore") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            block_max_maxscore_query block_max_maxscore_q(topk);
            block_max_maxscore_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "block_max_ranked_and") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            block_max_ranked_and_query block_max_ranked_and_q(topk);
            block_max_ranked_and_q(
                make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "ranked_and") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            ranked_and_query ranked_and_q(topk);
            ranked_and_q(make_scored_cursors(index, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "ranked_or") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            ranked_or_query ranked_or_q(topk);
            ranked_or_q(make_scored_cursors(index, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "maxscore") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            maxscore_query maxscore_q(topk);
            maxscore_q(make_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "ranked_or_taat") {
        query_fun =
            [&, accumulator = Simple_Accumulator(index.num_docs())](QueryRequest query) mutable {
                topk_queue topk(k);
                ranked_or_taat_query ranked_or_taat_q(topk);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query), index.num_docs(), accumulator);
                topk.finalize();
                return topk.topk();
            };
    } else if (query_type == "ranked_or_taat_lazy") {
        query_fun =
            [&, accumulator = Lazy_Accumulator<4>(index.num_docs())](QueryRequest query) mutable {
                topk_queue topk(k);
                ranked_or_taat_query ranked_or_taat_q(topk);
                ranked_or_taat_q(
                    make_scored_cursors(index, *scorer, query), index.num_docs(), accumulator);
                topk.finalize();
                return topk.topk();
            };
    } else if (query_type == "maxscore-uni") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            maxscore_uni_query q(topk);
            q(make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "maxscore-inter") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            if (not query.selection()) {
                spdlog::error("maxscore_inter_query requires posting list selections");
                std::exit(1);
            }
            maxscore_inter_query q(topk);
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            q(query, index, wdata, *pair_index, *scorer, index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "maxscore-inter-eager") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            if (not query.selection()) {
                spdlog::error("maxscore_inter_query requires posting list selections");
                std::exit(1);
            }
            maxscore_inter_eager_query q(topk);
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            q(query, index, wdata, *pair_index, *scorer, index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "maxscore-inter-opt") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            if (not query.selection()) {
                spdlog::error("maxscore_inter_query requires posting list selections");
                std::exit(1);
            }
            maxscore_inter_opt_query q(topk);
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            q(query, index, wdata, *pair_index, *scorer, index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else if (query_type == "block-max-union") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            BlockMaxUnionQuery q(topk);
            q(make_block_max_scored_cursors(index, wdata, *scorer, query), index.num_docs());
            topk.finalize();
            return topk.topk();
        };
    } else {
        spdlog::error("Unsupported query type: {}", query_type);
    }

    auto request_flags = RequestFlagSet::all();
    if (not use_thresholds) {
        request_flags.remove(RequestFlag::Threshold);
    }

    auto source = std::make_shared<mio::mmap_source>(documents_filename.c_str());
    auto docmap = Payload_Vector<>::from(*source);

    std::vector<std::vector<std::pair<float, uint64_t>>> raw_results(queries.size());
    auto start_batch = std::chrono::steady_clock::now();
    tbb::parallel_for(size_t(0), queries.size(), [&, query_fun](size_t query_idx) {
        raw_results[query_idx] = query_fun(queries[query_idx].query(k, request_flags));
    });
    auto end_batch = std::chrono::steady_clock::now();

    for (size_t query_idx = 0; query_idx < raw_results.size(); ++query_idx) {
        auto results = raw_results[query_idx];
        auto qid = queries[query_idx].id();
        for (auto&& [rank, result]: enumerate(results)) {
            std::cout << fmt::format(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                qid.value_or(std::to_string(query_idx)),
                iteration,
                docmap[result.second],
                rank,
                result.first,
                run_id);
        }
    }
    auto end_print = std::chrono::steady_clock::now();
    double batch_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_batch - start_batch).count();
    double batch_with_print_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_print - start_batch).count();
    spdlog::info("Time taken to process queries: {}ms", batch_ms);
    spdlog::info("Time taken to process queries with printing: {}ms", batch_with_print_ms);
}

class Inspect {
  public:
    void posting() { m_postings += 1; }
    void document() { m_documents += 1; }
    void lookup() { m_lookups += 1; }

    [[nodiscard]] auto to_json() const
    {
        return nlohmann::json{
            {"postings", m_postings}, {"documents", m_documents}, {"lookups", m_lookups}};
    }

  private:
    friend std::ostream& operator<<(std::ostream& os, Inspect const& inspect);

    std::size_t m_postings{0};
    std::size_t m_documents{0};
    std::size_t m_lookups{0};
};

std::ostream& operator<<(std::ostream& os, Inspect const& inspect)
{
    auto j = nlohmann::json{
        {"postings", inspect.m_postings},
        {"documents", inspect.m_documents},
        {"lookups", inspect.m_lookups}};
    os << j;
    return os;
}

template <typename IndexType, typename WandType>
void inspect(
    const std::string& index_filename,
    const std::optional<std::string>& wand_data_filename,
    const std::vector<QueryContainer>& queries,
    std::string const& type,
    std::string const& query_type,
    uint64_t k,
    std::string const& documents_filename,
    ScorerParams const& scorer_params,
    std::string const& run_id,
    std::string const& iteration,
    bool use_thresholds,
    std::optional<std::string>& pair_index_path)
{
    IndexType index;
    mio::mmap_source m(index_filename.c_str());
    mapper::map(index, m);

    WandType wdata;

    auto scorer = scorer::from_params(scorer_params, wdata);

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

    using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;
    auto pair_index = [&]() -> std::optional<pair_index_type> {
        if (pair_index_path) {
            return pair_index_type::load(*pair_index_path);
        }
        return std::nullopt;
    }();

    std::function<Inspect(QueryRequest)> query_fun;

    if (query_type == "wand" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "block_max_wand" && wand_data_filename) {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            block_max_wand_query block_max_wand_q(topk);
            Inspect inspect;
            block_max_wand_q(
                inspect_cursors(make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
                index.num_docs());
            return inspect;
        };
    } else if (query_type == "block_max_maxscore" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "block_max_ranked_and" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_and" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_or" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "maxscore" && wand_data_filename) {
        query_fun = [&](QueryRequest const& query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            maxscore_query maxscore_q(topk);
            Inspect inspect;
            maxscore_q(
                inspect_cursors(make_max_scored_cursors(index, wdata, *scorer, query), inspect),
                index.num_docs());
            return inspect;
        };
    } else if (query_type == "ranked_or_taat" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_or_taat_lazy" && wand_data_filename) {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "maxscore-uni" && wand_data_filename) {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            maxscore_uni_query q(topk);
            Inspect inspect;
            q(inspect_cursors(make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
              index.num_docs());
            return inspect;
        };
    } else if (query_type == "maxscore-inter" && wand_data_filename) {
        query_fun = [&](QueryRequest const& query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            Inspect inspect;
            if (not query.selection()) {
                spdlog::error("maxscore_inter_query requires posting list selections");
                std::exit(1);
            }
            auto selection = *query.selection();
            // if (selection.selected_pairs.empty()) {
            //    maxscore_uni_query q(topk);
            //    q(inspect_cursors(
            //          make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
            //      index.num_docs());
            //    return inspect;
            //}
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            maxscore_inter_query q(topk);
            q(query, index, wdata, *pair_index, *scorer, index.num_docs(), &inspect);
            return inspect;
        };
    } else if (query_type == "maxscore-inter-eager" && wand_data_filename) {
        query_fun = [&](QueryRequest const& query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            Inspect inspect;
            if (not query.selection()) {
                spdlog::error("maxscore_inter_query requires posting list selections");
                std::exit(1);
            }
            auto selection = *query.selection();
            // if (selection.selected_pairs.empty()) {
            //    maxscore_uni_query q(topk);
            //    q(inspect_cursors(
            //          make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
            //      index.num_docs());
            //    return inspect;
            //}
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            maxscore_inter_eager_query q(topk);
            q(query, index, wdata, *pair_index, *scorer, index.num_docs(), &inspect);
            return inspect;
        };
    } else if (query_type == "maxscore-inter-opt" && wand_data_filename) {
        query_fun = [&](QueryRequest const& query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            Inspect inspect;
            if (not query.selection()) {
                spdlog::error("maxscore-inter-opt requires posting list selections");
                std::exit(1);
            }
            auto selection = *query.selection();
            // if (selection.selected_pairs.empty()) {
            //    maxscore_uni_query q(topk);
            //    q(inspect_cursors(
            //          make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
            //      index.num_docs());
            //    return inspect;
            //}
            if (not pair_index) {
                spdlog::error("Must provide pair index for maxscore-inter");
                std::exit(1);
            }
            maxscore_inter_opt_query q(topk);
            q(query, index, wdata, *pair_index, *scorer, index.num_docs(), &inspect);
            return inspect;
        };
    } else {
        spdlog::error("Unsupported query type: {}", query_type);
    }

    auto request_flags = RequestFlagSet::all();
    if (not use_thresholds) {
        request_flags.remove(RequestFlag::Threshold);
    }

    for (auto&& query: queries) {
        auto inspect = query_fun(query.query(k, request_flags));
        auto output = inspect.to_json();
        output["query"] = query.to_json();
        std::cout << output << '\n';
    }
}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed<>>;
using wand_uniform_index_quantized = wand_data<wand_data_compressed<PayloadType::Quantized>>;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string documents_file;
    std::string run_id = "R0";
    bool quantized = false;
    bool use_thresholds = false;
    bool inspect = false;
    std::optional<std::string> pair_index_path{};

    App<arg::Index,
        arg::WandData<arg::WandMode::Required>,
        arg::Query<arg::QueryMode::Ranked>,
        arg::Algorithm,
        arg::Scorer,
        arg::Thresholds,
        arg::Threads>
        app{"Retrieves query results in TREC format."};
    app.add_option("-r,--run", run_id, "Run identifier");
    auto group = app.add_option_group("documents");
    group->add_option("--documents", documents_file, "Document lexicon");
    group->add_flag("--inspect", inspect, "Inspect for statistics such as no. postings or lookups");
    group->require_option();
    app.add_flag("--quantized", quantized, "Quantized scores");
    app.add_flag(
        "--use-thresholds",
        use_thresholds,
        "Initialize top-k queue with threshold passed as part of a query object");
    app.add_option("--pair-index", pair_index_path, "Path to pair index.");

    CLI11_PARSE(app, argc, argv);

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, app.threads() + 1);
    spdlog::info("Number of worker threads: {}", app.threads());

    if (run_id.empty()) {
        run_id = "PISA";
    }

    auto iteration = "Q0";

    std::vector<pisa::QueryContainer> queries;
    try {
        queries = app.resolved_queries();
        for (auto& query: queries) {
            query.add_threshold(app.k(), *query.threshold(app.k()) - 0.001);
        }
    } catch (pisa::MissingResolverError err) {
        spdlog::error("Unresoved queries (without IDs) require term lexicon.");
        std::exit(1);
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        std::exit(1);
    }

    auto params = std::make_tuple(
        app.index_filename(),
        app.wand_data_path(),
        queries,
        app.index_encoding(),
        app.algorithm(),
        app.k(),
        documents_file,
        app.scorer_params(),
        run_id,
        iteration,
        use_thresholds,
        pair_index_path);

    if (inspect) {
        /**/
        if (false) {  // NOLINT
#define LOOP_BODY(R, DATA, T)                                                                  \
    }                                                                                          \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                    \
    {                                                                                          \
        if (app.is_wand_compressed()) {                                                        \
            if (quantized) {                                                                   \
                std::apply(                                                                    \
                    ::inspect<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>, params); \
            } else {                                                                           \
                std::apply(::inspect<BOOST_PP_CAT(T, _index), wand_uniform_index>, params);    \
            }                                                                                  \
        } else {                                                                               \
            std::apply(::inspect<BOOST_PP_CAT(T, _index), wand_raw_index>, params);            \
        }                                                                                      \
        /**/

            BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
        } else {
            spdlog::error("Unknown type {}", app.index_encoding());
        }
    } else {
        /**/
        if (false) {  // NOLINT
#define LOOP_BODY(R, DATA, T)                                                                      \
    }                                                                                              \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                        \
    {                                                                                              \
        if (app.is_wand_compressed()) {                                                            \
            if (quantized) {                                                                       \
                std::apply(                                                                        \
                    evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>,       \
                    params);                                                                       \
            } else {                                                                               \
                std::apply(evaluate_queries<BOOST_PP_CAT(T, _index), wand_uniform_index>, params); \
            }                                                                                      \
        } else {                                                                                   \
            std::apply(evaluate_queries<BOOST_PP_CAT(T, _index), wand_raw_index>, params);         \
        }                                                                                          \
        /**/

            BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
        } else {
            spdlog::error("Unknown type {}", app.index_encoding());
        }
    }
}
