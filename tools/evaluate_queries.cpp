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

struct RunArgs {
    std::string index_filename;
    std::string wand_data_filename;
    std::vector<QueryContainer> queries;
    std::string index_encoding;
    std::string algorithm;
    uint64_t k;
    std::string documents_lexicon;
    ScorerParams scorer_params;

    bool use_thresholds = false;
    bool disk_resident = false;

    std::string run_id = "PISA";
    std::string iteration = "Q0";

    std::optional<std::string> pair_index_path{};

    template <typename App>
    RunArgs(App&& app, std::vector<pisa::QueryContainer> queries, std::string documents_lexicon)
        : index_filename(app.index_filename()),
          wand_data_filename(app.wand_data_path()),
          queries(std::move(queries)),
          index_encoding(app.index_encoding()),
          algorithm(app.algorithm()),
          k(app.k()),
          documents_lexicon(std::move(documents_lexicon)),
          scorer_params(app.scorer_params())
    {}
};

using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;

template <typename IndexType, typename WandType, typename Scorer>
void evaluate_queries(
    IndexType&& index,
    WandType&& wdata,
    Scorer&& scorer,
    std::optional<pair_index_type> const& pair_index,
    RunArgs const& params)
{
    std::function<std::vector<std::pair<float, uint64_t>>(QueryRequest)> query_fun;

    auto const& query_type = params.algorithm;
    auto const& k = params.k;

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
    if (not params.use_thresholds) {
        request_flags.remove(RequestFlag::Threshold);
    }

    auto source = std::make_shared<mio::mmap_source>(params.documents_lexicon.c_str());
    auto docmap = Payload_Vector<>::from(*source);

    auto const& queries = params.queries;
    std::vector<std::vector<std::pair<float, uint64_t>>> raw_results(queries.size());
    auto start_batch = std::chrono::steady_clock::now();
    tbb::parallel_for(size_t(0), queries.size(), [&, query_fun](size_t query_idx) {
        raw_results[query_idx] = query_fun(queries[query_idx].query(k, request_flags));
    });
    auto end_batch = std::chrono::steady_clock::now();

    for (size_t query_idx = 0; query_idx < raw_results.size(); ++query_idx) {
        std::cerr << query_idx << '\n';
        auto results = raw_results[query_idx];
        auto qid = queries[query_idx].id();
        for (auto&& [rank, result]: enumerate(results)) {
            std::cout << fmt::format(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                qid.value_or(std::to_string(query_idx)),
                params.iteration,
                docmap[result.second],
                rank,
                result.first,
                params.run_id);
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

template <typename IndexType, typename WandType, typename Scorer>
void inspect(
    IndexType&& index,
    WandType&& wdata,
    Scorer&& scorer,
    std::optional<pair_index_type> const& pair_index,
    RunArgs const& params)
{
    std::function<Inspect(QueryRequest)> query_fun;

    auto const& query_type = params.algorithm;
    auto const& k = params.k;

    if (query_type == "wand") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "block_max_wand") {
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
    } else if (query_type == "block_max_maxscore") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "block_max_ranked_and") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_and") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_or") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "maxscore") {
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
    } else if (query_type == "ranked_or_taat") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "ranked_or_taat_lazy") {
        spdlog::error("Unimplemented");
        std::abort();
    } else if (query_type == "maxscore-uni") {
        query_fun = [&](QueryRequest query) {
            topk_queue topk(k);
            topk.set_threshold(query.threshold().value_or(0));
            maxscore_uni_query q(topk);
            Inspect inspect;
            q(inspect_cursors(make_block_max_scored_cursors(index, wdata, *scorer, query), inspect),
              index.num_docs());
            return inspect;
        };
    } else if (query_type == "maxscore-inter") {
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
    } else if (query_type == "maxscore-inter-eager") {
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
    } else if (query_type == "maxscore-inter-opt") {
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
    if (not params.use_thresholds) {
        request_flags.remove(RequestFlag::Threshold);
    }

    for (auto&& query: params.queries) {
        auto inspect = query_fun(query.query(k, request_flags));
        auto output = inspect.to_json();
        output["query"] = query.to_json();
        std::cout << output << '\n';
    }
}

template <typename IndexType, typename WandType>
void run(RunArgs params, bool inspect)
{
    auto index = [&]() {
        if (params.disk_resident) {
            return IndexType(MemorySource::disk_resident_file(params.index_filename));
        }
        return IndexType(MemorySource::mapped_file(params.index_filename));
    }();
    WandType const wdata(MemorySource::mapped_file(params.wand_data_filename));

    auto scorer = scorer::from_params(params.scorer_params, wdata);

    using pair_index_type = PairIndex<block_freq_index<simdbp_block, false, IndexArity::Binary>>;
    auto pair_index = [&]() -> std::optional<pair_index_type> {
        if (params.pair_index_path) {
            return pair_index_type::load(*params.pair_index_path, true);
        }
        return std::nullopt;
    }();

    if (inspect) {
        ::inspect(index, wdata, scorer, pair_index, std::move(params));
    } else {
        ::evaluate_queries(index, wdata, scorer, pair_index, std::move(params));
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
    bool disk_resident = false;
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
    app.add_flag(
        "--disk-resident", disk_resident, "Keep index on disk and load postings at query time.");

    CLI11_PARSE(app, argc, argv);

    tbb::global_control control(tbb::global_control::max_allowed_parallelism, app.threads() + 1);
    spdlog::info("Number of worker threads: {}", app.threads());

    std::vector<pisa::QueryContainer> queries;
    try {
        queries = app.resolved_queries();
        for (auto& query: queries) {
            if (auto threshold = query.threshold(app.k()); threshold && *threshold > 0.0) {
                if (quantized) {
                    query.add_threshold(app.k(), *threshold - 1.0);
                } else {
                    query.add_threshold(app.k(), std::nextafter(*threshold, 0.0));
                }
            }
        }
    } catch (pisa::MissingResolverError err) {
        spdlog::error("Unresoved queries (without IDs) require term lexicon.");
        std::exit(1);
    } catch (std::runtime_error const& err) {
        spdlog::error(err.what());
        std::exit(1);
    }

    RunArgs params(app, std::move(queries), std::move(documents_file));
    if (!run_id.empty()) {
        params.run_id = run_id;
    }
    params.use_thresholds = use_thresholds;
    params.disk_resident = disk_resident;
    params.pair_index_path = pair_index_path;

    /**/
    if (false) {  // NOLINT

#define LOOP_BODY(R, DATA, T)                                                                   \
    }                                                                                           \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                                     \
    {                                                                                           \
        if (app.is_wand_compressed()) {                                                         \
            if (quantized) {                                                                    \
                ::run<BOOST_PP_CAT(T, _index), wand_uniform_index_quantized>(                   \
                    std::move(params), inspect);                                                \
            } else {                                                                            \
                ::run<BOOST_PP_CAT(T, _index), wand_uniform_index>(std::move(params), inspect); \
            }                                                                                   \
        } else {                                                                                \
            ::run<BOOST_PP_CAT(T, _index), wand_raw_index>(std::move(params), inspect);         \
        }                                                                                       \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }
}
