#include <fstream>
#include <iostream>
#include <algorithm>
#include <thread>
#include <numeric>

#include <boost/lexical_cast.hpp>
#include <boost/filesystem/operations.hpp>

#include <succinct/mapper.hpp>

#include <stxxl/vector>
#include <stxxl/io>
#include <stxxl/sort>

#include "configuration.hpp"
#include "index_types.hpp"
#include "util.hpp"
#include "verify_collection.hpp"
#include "mixed_block.hpp"
#include "index_build_utils.hpp"

using ds2i::logger;

typedef uint32_t block_id_type; // XXX for memory reasons, but would need size_t for very large indexes

struct lambda_point {
    block_id_type block_id;
    float lambda;
    ds2i::mixed_block::space_time_point st;

    struct comparator {
        bool operator()(lambda_point const& lhs, lambda_point const& rhs) const
        {
            return lhs.lambda < rhs.lambda;
        }

        static lambda_point min_value()
        {
            lambda_point val;
            val.lambda = std::numeric_limits<float>::min();
            return val;
        }

        static lambda_point max_value()
        {
            lambda_point val;
            val.lambda = std::numeric_limits<float>::max();
            return val;
        }
    };
};

typedef stxxl::vector<lambda_point> lambda_vector_type;

template <typename InputCollectionType>
struct lambdas_computer : ds2i::semiasync_queue::job {
    lambdas_computer(block_id_type block_id_base,
                     typename InputCollectionType::document_enumerator e,
                     ds2i::predictors_vec_type const& predictors,
                     std::vector<uint32_t>& counts,
                     ds2i::progress_logger& plog,
                     lambda_vector_type& lambda_points)
        : m_block_id_base(block_id_base)
        , m_e(e)
        , m_predictors(predictors)
        , m_plog(plog)
        , m_lambda_points(lambda_points)
    {
        m_counts.swap(counts);
    }

    virtual void prepare()
    {
        using namespace ds2i;
        using namespace time_prediction;

        auto blocks = m_e.get_blocks();
        assert(m_counts.empty() || m_counts.size() == 2 * blocks.size());

        bool heuristic_greedy = configuration::get().heuristic_greedy;

        block_id_type cur_block_id = m_block_id_base;
        for (auto const& input_block: blocks) {
            static const uint32_t smoothing = 1; // Laplace smoothing
            uint32_t docs_exp = smoothing, freqs_exp = smoothing;
            if (!m_counts.empty()) {
                docs_exp += m_counts[2 * input_block.index];
                freqs_exp += m_counts[2 * input_block.index + 1];
            }

            thread_local std::vector<uint32_t> values;

            auto append_lambdas = [&](std::vector<mixed_block::space_time_point>& points,
                                      block_id_type block_id) {
                // sort by space, time
                std::sort(points.begin(), points.end());

                // smallest point is always added with lambda=0
                m_points_buf.push_back(lambda_point { block_id, 0, points.front() });
                for (auto const& cur: points) {
                    while (true) {
                        auto const& prev = m_points_buf.back();
                        // if this point is dominated we can skip it
                        if (cur.time >= prev.st.time) break;
                        auto lambda = (cur.space - prev.st.space) / (prev.st.time - cur.time);
                        if (!heuristic_greedy && lambda < prev.lambda) {
                            m_points_buf.pop_back();
                        } else {
                            m_points_buf.push_back(lambda_point { block_id, lambda, cur });
                            break;
                        }
                    }
                }
            };

            input_block.decode_doc_gaps(values);
            auto docs_sts = mixed_block::compute_space_time(values, input_block.doc_gaps_universe,
                                                            m_predictors, docs_exp);
            append_lambdas(docs_sts, cur_block_id++);

            input_block.decode_freqs(values);
            auto freqs_sts = mixed_block::compute_space_time(values, uint32_t(-1),
                                                             m_predictors, freqs_exp);
            append_lambdas(freqs_sts, cur_block_id++);
        }

        succinct::util::dispose(m_counts);
    }

    virtual void commit()
    {
        // m_lambda_points.insert(m_lambda_points.end(),
        //                        m_points_buf.begin(), m_points_buf.end());
        std::copy(m_points_buf.begin(), m_points_buf.end(),
                  std::back_inserter(m_lambda_points));
        m_plog.done_sequence(m_e.size());
    }

    block_id_type m_block_id_base;
    typename InputCollectionType::document_enumerator m_e;
    ds2i::predictors_vec_type const& m_predictors;
    std::vector<uint32_t> m_counts;
    ds2i::progress_logger& m_plog;
    double m_lambda;
    std::vector<lambda_point> m_points_buf;
    lambda_vector_type& m_lambda_points;
};

template <typename InputCollectionType>
void compute_lambdas(InputCollectionType const& input_coll,
                     size_t num_blocks,
                     const char* predictors_filename,
                     const char* block_stats_filename,
                     const char* lambdas_filename)

{
    using namespace ds2i;
    using namespace time_prediction;

    logger() << "Computing lambdas" << std::endl;
    progress_logger plog;

    auto predictors = load_predictors(predictors_filename);
    std::ifstream block_stats(block_stats_filename);

    double tick = get_time_usecs();
    double user_tick = get_user_time_usecs();

    std::string line;
    uint32_t block_counts_list;
    std::vector<uint32_t> block_counts;
    bool block_counts_consumed = true;
    block_id_type block_id_base = 0;
    size_t freq_zero_lists = 0;
    size_t freq_zero_blocks = 0;

    stxxl::syscall_file lpfile(lambdas_filename,
                               stxxl::file::DIRECT | stxxl::file::CREAT | stxxl::file::RDWR);
    lambda_vector_type lambda_points(&lpfile);

    semiasync_queue queue(1 << 24);

    for (size_t l = 0; l < input_coll.size(); ++l) {
        if (block_counts_consumed) {
            block_counts_consumed = false;
            read_block_stats(block_stats, block_counts_list, block_counts);
        }

        auto e = input_coll[l];

        typedef lambdas_computer<InputCollectionType> job_type;
        std::shared_ptr<job_type> job;

        if (l == block_counts_list) {
            freq_zero_blocks += std::accumulate(block_counts.begin(), block_counts.end(), size_t(0),
                                                [](size_t accum, uint32_t freq) {
                                                    return accum + (freq == 0);
                                                });
            block_counts_consumed = true;
            job.reset(new job_type(block_id_base, e, predictors, block_counts, plog, lambda_points));
        } else {
            freq_zero_lists += 1;
            freq_zero_blocks += 2 * e.num_blocks();
            std::vector<uint32_t> empty_counts;
            job.reset(new job_type(block_id_base, e, predictors, empty_counts, plog, lambda_points));
        }

        block_id_base += 2 * e.num_blocks();
        queue.add_job(job, 2 * e.size());
    }

    assert(block_id_base == num_blocks); (void)num_blocks;

    stats_line()
        ("freq_zero_lists", freq_zero_lists)
        ("freq_zero_blocks", freq_zero_blocks)
        ;

    queue.complete();
    plog.log();

    logger() << lambda_points.size() << " lambda points" << std::endl;
    logger() << "Sorting lambda points" << std::endl;
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    double user_elapsed_secs = (get_user_time_usecs() - user_tick) / 1000000;

    stats_line()
        ("worker_threads", configuration::get().worker_threads)
        ("lambda_computation_time", elapsed_secs)
        ("lambda_computation_user_time", user_elapsed_secs)
        ("is_heuristic", configuration::get().heuristic_greedy)
        ;

    tick = get_time_usecs();
    user_tick = get_user_time_usecs();
    static const size_t sort_memory = size_t(16) * 1024 * 1024 * 1024; // XXX
    stxxl::sort(lambda_points.begin(), lambda_points.end(),
                lambda_point::comparator(),
                sort_memory);

    elapsed_secs = (get_time_usecs() - tick) / 1000000;
    user_elapsed_secs = (get_user_time_usecs() - user_tick) / 1000000;
    stats_line()
        ("worker_threads", configuration::get().worker_threads)
        ("lambda_sorting_time", elapsed_secs)
        ("lambda_sorting_user_time", user_elapsed_secs)
        ("is_heuristic", configuration::get().heuristic_greedy)
        ;
}

template <typename InputCollectionType, typename CollectionBuilder>
struct list_transformer : ds2i::semiasync_queue::job {
    list_transformer(CollectionBuilder& b,
                     typename InputCollectionType::document_enumerator e,
                     std::vector<ds2i::mixed_block::block_type>::const_iterator block_type_begin,
                     std::vector<ds2i::mixed_block::compr_param_type>::const_iterator block_param_begin,
                     ds2i::progress_logger& plog)
        : m_b(b)
        , m_e(e)
        , m_block_type(block_type_begin)
        , m_block_param(block_param_begin)
        , m_plog(plog)
    {}

    virtual void prepare()
    {
        using namespace ds2i;

        typedef typename InputCollectionType::document_enumerator::block_data input_block_type;
        typedef mixed_block::block_transformer<input_block_type> output_block_type;

        auto blocks = m_e.get_blocks();
        std::vector<output_block_type> output_blocks;

        for (auto const& input_block: blocks) {
            auto docs_type = *m_block_type++;
            auto freqs_type = *m_block_type++;
            auto docs_param = *m_block_param++;
            auto freqs_param = *m_block_param++;
            output_blocks.emplace_back(input_block,
                                       docs_type, freqs_type,
                                       docs_param, freqs_param);
        }

        block_posting_list<mixed_block>::write_blocks(m_buf, m_e.size(), output_blocks);
    }

    virtual void commit()
    {
        m_b.add_posting_list(m_buf);
        m_plog.done_sequence(m_e.size());
    }

    CollectionBuilder& m_b;
    typename InputCollectionType::document_enumerator m_e;
    std::vector<ds2i::mixed_block::block_type>::const_iterator m_block_type;
    std::vector<ds2i::mixed_block::compr_param_type>::const_iterator m_block_param;
    ds2i::progress_logger& m_plog;
    std::vector<uint8_t> m_buf;
};


template <typename InputCollectionType>
void optimal_hybrid_index(ds2i::global_parameters const& params,
                          const char* predictors_filename,
                          const char* block_stats_filename,
                          const char* input_filename,
                          const char* output_filename,
                          const char* lambdas_filename,
                          size_t budget)
{
    using namespace ds2i;

    InputCollectionType input_coll;
    boost::iostreams::mapped_file_source m(input_filename);
    succinct::mapper::map(input_coll, m);

    logger() << "Processing " << input_coll.size() << " posting lists" << std::endl;
    size_t num_blocks = 0;
    size_t partial_blocks = 0;
    size_t space_base = 8; // space overhead independent of block compression method
    for (size_t l = 0; l < input_coll.size(); ++l) {
        auto e = input_coll[l];
        num_blocks += 2 * e.num_blocks();
        // list length in vbyte
        space_base += succinct::util::ceil_div(succinct::broadword::msb(e.size()) + 1, 7);
        space_base += e.num_blocks() * 4; // max docid
        space_base += (e.num_blocks() - 1) * 4; // endpoint
        if (e.size() % mixed_block::block_size != 0) {
            partial_blocks += 2;
        }
    }

    logger() << num_blocks << " overall blocks" << std::endl;

    if (boost::filesystem::exists(lambdas_filename)) {
        logger() << "Found lambdas file " << lambdas_filename << ", skipping recomputation" << std::endl;
        logger() << "To recompute lambdas, remove file" << std::endl;
    } else {
        compute_lambdas(input_coll, num_blocks, predictors_filename,
                        block_stats_filename, lambdas_filename);
    }

    stxxl::syscall_file lpfile(lambdas_filename,
                               stxxl::file::DIRECT | stxxl::file::RDONLY);
    lambda_vector_type lambda_points(&lpfile);

    double tick = get_time_usecs();
    double user_tick = get_user_time_usecs();

    logger() << "Computing space-time tradeoffs" << std::endl;
    std::vector<uint16_t> block_spaces(num_blocks);
    std::vector<float> block_times(num_blocks);
    std::vector<mixed_block::block_type> block_types(num_blocks);
    std::vector<mixed_block::compr_param_type> block_params(num_blocks);
    size_t cur_space = space_base;
    double cur_time = 0;
    size_t seen_lambdas = 0;
    float first_nonzero_lambda = true;

    std::ofstream lambdas_log;
    if (budget == 0) {
        lambdas_log.open(output_filename, std::ios::out);
    }

    for (auto const& lpid: lambda_vector_type::bufreader_type(lambda_points)) {
        assert(lpid.block_id < num_blocks);
        cur_space -= block_spaces[lpid.block_id];
        cur_time -= block_times[lpid.block_id];

        block_spaces[lpid.block_id] = lpid.st.space;
        block_times[lpid.block_id] = lpid.st.time;
        block_types[lpid.block_id] = lpid.st.type;
        block_params[lpid.block_id] = lpid.st.param;

        cur_space += block_spaces[lpid.block_id];
        cur_time += block_times[lpid.block_id];

        if (lpid.lambda > 0) { // we are past the initial frontier
            if (first_nonzero_lambda) {
                logger() << "Minimum feasible space: " << cur_space << std::endl;
                first_nonzero_lambda = false;
            }

            if (budget == 0) {
                // just print out a sample of the trade-offs
                if (seen_lambdas % (num_blocks / 2000) == 0) {
                    lambdas_log << lpid.lambda << '\t' << cur_space << '\t' << cur_time << '\n';
                }
                seen_lambdas += 1;
            } else if (cur_space > budget) { // XXX replace with >=
                break;
            }
        }
    }

    succinct::util::dispose(block_spaces);
    succinct::util::dispose(block_times);

    if (budget == 0) {
        logger() << "Done" << std::endl;
        return; // done, just reporting the trade-offs
    }

    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    double user_elapsed_secs = (get_user_time_usecs() - user_tick) / 1000000;
    stats_line()
        ("worker_threads", configuration::get().worker_threads)
        ("greedy_time", elapsed_secs)
        ("greedy_user_time", user_elapsed_secs)
        ;

    logger() << "Found trade-off. Space: " << cur_space
             << " Time: " << cur_time << std::endl;

    stats_line()
        ("found_space", cur_space)
        ("found_time", cur_time)
        ;

    typedef std::tuple<uint32_t, uint32_t> type_param_pair;
    std::map<type_param_pair, size_t> type_counts;
    for (size_t i = 0; i < num_blocks; ++i) {
        type_counts[type_param_pair((uint8_t)block_types[i], block_params[i])] += 1;
    }

    std::vector<std::pair<type_param_pair, size_t>> type_counts_vec;
    for (uint8_t t = 0; t < mixed_block::block_types; ++t) {
        for (uint8_t param = 0; param < mixed_block::compr_params((mixed_block::block_type)t); ++param) {
            auto tp = type_param_pair(t, param);
            type_counts_vec.push_back(std::make_pair(tp, type_counts[tp]));
        }
    }

    stats_line()
        ("blocks", num_blocks)
        ("partial_blocks", partial_blocks)
        ("type_counts", type_counts_vec)
        ;

    tick = get_time_usecs();
    user_tick = get_user_time_usecs();

    typedef typename block_mixed_index::builder builder_type;
    builder_type builder(input_coll.num_docs(), params);
    progress_logger plog;
    semiasync_queue queue(1 << 24);
    auto block_types_it = block_types.begin();
    auto block_params_it = block_params.begin();

    for (size_t l = 0; l < input_coll.size(); ++l) {
        auto e = input_coll[l];

        typedef list_transformer<InputCollectionType, builder_type> job_type;
        std::shared_ptr<job_type> job(new job_type(builder, e,
                                                   block_types_it,
                                                   block_params_it,
                                                   plog));

        block_types_it += 2 * e.num_blocks();
        block_params_it += 2 * e.num_blocks();
        queue.add_job(job, 2 * e.size());
    }

    assert(block_types_it == block_types.end());
    assert(block_params_it == block_params.end());
    queue.complete();
    plog.log();

    block_mixed_index coll;
    builder.build(coll);
    elapsed_secs = (get_time_usecs() - tick) / 1000000;
    user_elapsed_secs = (get_user_time_usecs() - user_tick) / 1000000;
    logger() << "Collection built in "
             << elapsed_secs << " seconds" << std::endl;

    stats_line()
        ("worker_threads", configuration::get().worker_threads)
        ("construction_time", elapsed_secs)
        ("construction_user_time", user_elapsed_secs)
        ;
    dump_stats(coll, "block_mixed", plog.postings);

    if (output_filename) {
        succinct::mapper::freeze(coll, output_filename);
    }
}


int main(int argc, const char** argv) {

    using namespace ds2i;

    if (argc < 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <index type> <predictors> <block_stats> <input_index> <lambdas_filename> <budget> [output_index] [--check <collection_basename>]"
                  << std::endl;
        return 1;
    }

    std::string type = argv[1];
    const char* predictors_filename = argv[2];
    const char* block_stats_filename = argv[3];
    const char* input_filename = argv[4];
    const char* lambdas_filename = argv[5];
    size_t budget = boost::lexical_cast<size_t>(argv[6]);
    const char* output_filename = nullptr;
    if (argc > 7) {
        output_filename = argv[7];
    }

    bool check = false;
    const char* collection_basename = nullptr;
    if (argc > 9 && std::string(argv[8]) == "--check") {
        check = true;
        collection_basename = argv[9];
    }

    ds2i::global_parameters params;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                           \
        } else if (type == BOOST_PP_STRINGIZE(T)) {                     \
            optimal_hybrid_index<BOOST_PP_CAT(T, _index)>               \
                (params, predictors_filename, block_stats_filename,     \
                 input_filename, output_filename, lambdas_filename, budget); \
            if (check) {                                                \
                binary_freq_collection input(collection_basename);      \
                verify_collection<binary_freq_collection, block_mixed_index> \
                                  (input, output_filename);             \
            }                                                           \
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_BLOCK_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

    return 0;
}
