#include <iostream>

#include <succinct/mapper.hpp>

#include "index_types.hpp"
#include "wand_data.hpp"
#include "queries.hpp"
#include "util.hpp"

template <typename IndexType>
void selective_queries(const char* index_filename,
                       std::string const& type)
{
    using namespace ds2i;

    IndexType index;
    logger() << "Loading index from " << index_filename << std::endl;
    boost::iostreams::mapped_file_source m(index_filename);
    succinct::mapper::map(index, m, succinct::mapper::map_flags::warmup);

    logger() << "Performing " << type << " queries" << std::endl;

    term_id_vec query;
    while (read_query(query)) {
        uint64_t and_results = and_query<false>()(index, query);
        uint64_t or_results = or_query<false>()(index, query);

        double selectiveness = double(and_results) / double(or_results);
        if (selectiveness < 0.005) {
            std::cout << query[0];
            for (size_t i = 1; i < query.size(); ++i) std::cout << " " << query[i];
            std::cout << std::endl;
        }
    }
}

int main(int, const char** argv)
{
    using namespace ds2i;

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
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

}
