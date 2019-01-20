#include <iostream>

#include "succinct/mapper.hpp"
#include "mio/mmap.hpp"

#include "index_types.hpp"
#include "wand_data_compressed.hpp"
#include "query/queries.hpp"
#include "util/util.hpp"

template <typename IndexType>
void selective_queries(const char* index_filename,
                       std::string const& type)
{
    using namespace pisa;


    IndexType index;
    spdlog::info("Loading index from {}", index_filename);
    mio::mmap_source m(index_filename);
    mapper::map(index, m, mapper::map_flags::warmup);

    spdlog::info("Performing {} queries", type);

    term_id_vec query;


    uint64_t count_taken = 0;
    uint64_t count = 0;
    while (read_query(query)) {

        bool insert = true;
        if (query.size() == 1)
            insert = false;
        else {
            count++;
            for (term_id_type term : query) {
                auto t = index[term];
                if (t.size() <= configuration::get().threshold_wand_list) {
                    insert = false;
                    break;
                }
            }
        }



        if (insert) {
            count_taken++;
            std::cout << query[0];
            for (size_t i = 1; i < query.size(); ++i) std::cout << " " << query[i];
            std::cout << std::endl;
        }
    }

    std::cout << (float) count_taken / (float) count << std::endl;

}


int main(int, const char** argv) {
    using namespace pisa;

    std::string type = argv[1];
    const char* index_filename = argv[2];

    if (false) {
#define LOOP_BODY(R, DATA, T)                           \
        } else if (type == BOOST_PP_STRINGIZE(T)) {     \
            selective_queries<BOOST_PP_CAT(T, _index)>  \
                (index_filename, type);
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", type);
    }

}
