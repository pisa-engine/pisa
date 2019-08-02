#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <mio/mmap.hpp>
#include <range/v3/view/enumerate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

#include "mappable/mapper.hpp"

#include "index_types.hpp"
#include "accumulator/lazy_accumulator.hpp"
#include "query/queries.hpp"
#include "timer.hpp"
#include "util/util.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"
#include "cursor/cursor.hpp"
#include "cursor/scored_cursor.hpp"
#include "cursor/max_scored_cursor.hpp"
#include "cursor/block_max_scored_cursor.hpp"

#include "CLI/CLI.hpp"


 
using namespace pisa;

template <typename IndexType, typename WandType>
void block_error(const std::string &index_filename,
                 const std::optional<std::string> &wand_data_filename,
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

    const uint64_t max_docid = index.num_docs();

    // For each term in the index
    for (size_t list = 0; list < index.size(); ++list) {
        double list_error = 0.0f;
        auto posting = index[list];
        auto wand = wdata.getenum(list);
        auto q_weight = 0; // XXX
        uint64_t cur_id = posting.docid();
        while (cur_id < max_docid) {
            wand.next_geq(cur_id);
            double block_upper_bound = wand.score() * q_weight;
            double doc_score = 0; // XXX
            list_error += block_upper_bound - doc_score; 
            cur_id = posting.next();
        }    
    }

}

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

int main(int argc, const char **argv)
{
    std::string type;
    std::string index_filename;
    std::string wand_data_filename;
    bool compressed = false;

    CLI::App app{"evaluate_block_error - a tool for measuring the error between block_max and true score values."};
    app.add_option("-t,--type", type, "Index type")->required();
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename")->required();
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    CLI11_PARSE(app, argc, argv);

    spdlog::set_default_logger(spdlog::stderr_color_mt("stderr"));

     /**/
    if (false) {
#define LOOP_BODY(R, DATA, T)                                       \
    }                                                               \
    else if (type == BOOST_PP_STRINGIZE(T))                         \
    {                                                               \
        if (compressed) {                                           \
            block_error<BOOST_PP_CAT(T, _index), wand_uniform_index>( \
                index_filename, wand_data_filename, type); \
        } else {                                                    \
            block_error<BOOST_PP_CAT(T, _index), wand_raw_index>(     \
                index_filename, wand_data_filename, type); \
        }                                                           \
        /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY

    } else {
        spdlog::error("Unknown type {}", type);
    }

}
