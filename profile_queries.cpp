#include <iostream>
#include <thread>

#include <succinct/mapper.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "index_types.hpp"
#include "wand_data.hpp"
#include "queries.hpp"
#include "util.hpp"

template <typename QueryOperator, typename IndexType>
void op_profile(IndexType const& index,
                QueryOperator const& query_op,
                std::vector<ds2i::term_id_vec> const& queries)
{
    using namespace ds2i;

    size_t n_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads(n_threads);
    std::mutex io_mutex;

    for (size_t tid = 0; tid < n_threads; ++tid) {
        threads[tid] = std::thread([&, tid]() {
                auto query_op_copy = query_op; // copy one query_op per thread
                for (size_t i = tid; i < queries.size(); i += n_threads) {
                    if (i % 10000 == 0) {
                        std::lock_guard<std::mutex> lock(io_mutex);
                        logger() << i << " queries processed" << std::endl;
                    }

                    query_op_copy(index, queries[i]);
                }
            });
    }

    for (auto& thread: threads) thread.join();
}

template <typename IndexType>
struct add_profiling { typedef IndexType type; };

template <typename BlockType>
struct add_profiling<ds2i::block_freq_index<BlockType, false>> {
    typedef ds2i::block_freq_index<BlockType, true> type;
};


template <typename IndexType>
void profile(const char* index_filename,
             const char* wand_data_filename,
             std::vector<ds2i::term_id_vec> const& queries,
             std::string const& type,
             std::string const& query_type)
{
    using namespace ds2i;

    typename add_profiling<IndexType>::type index;
    logger() << "Loading index from " << index_filename << std::endl;
    boost::iostreams::mapped_file_source m(index_filename);
    succinct::mapper::map(index, m);

    wand_data<> wdata;
    boost::iostreams::mapped_file_source md;
    if (wand_data_filename) {
        md.open(wand_data_filename);
        succinct::mapper::map(wdata, md, succinct::mapper::map_flags::warmup);
    }

    logger() << "Performing " << type << " queries" << std::endl;

    std::vector<std::string> query_types;
    boost::algorithm::split(query_types, query_type, boost::is_any_of(":"));

    for (auto const& t: query_types) {
        logger() << "Query type: " << t << std::endl;
        if (t == "and") {
            op_profile(index, and_query<false>(), queries);
        } else if (t == "ranked_and" && wand_data_filename) {
            op_profile(index, ranked_and_query(wdata, 10), queries);
        } else if (t == "wand" && wand_data_filename) {
            op_profile(index, wand_query(wdata, 10), queries);
        } else if (t == "maxscore" && wand_data_filename) {
            op_profile(index, maxscore_query(wdata, 10), queries);
        } else {
            logger() << "Unsupported query type: " << t << std::endl;
        }
    }

    block_profiler::dump(std::cout);
}

int main(int argc, const char** argv)
{
    using namespace ds2i;

    std::string type = argv[1];
    const char* query_type = argv[2];
    const char* index_filename = argv[3];
    const char* wand_data_filename = nullptr;
    if (argc > 4) {
        wand_data_filename = argv[4];
    }

    std::vector<term_id_vec> queries;
    term_id_vec q;
    while (read_query(q)) queries.push_back(q);

    if (false) {
#define LOOP_BODY(R, DATA, T)                                   \
        } else if (type == BOOST_PP_STRINGIZE(T)) {             \
            profile<BOOST_PP_CAT(T, _index)>                    \
                (index_filename, wand_data_filename, queries,   \
                 type, query_type);                             \
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

}
