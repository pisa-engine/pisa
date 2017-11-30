#include <iostream>
#include <random>

#include <boost/lexical_cast.hpp>

#include "succinct/mapper.hpp"
#include "index_types.hpp"
#include "util.hpp"
#include "dec_time_prediction.hpp"

namespace ds2i {

    double measure_decoding_time(size_t sum_of_values, size_t n,
                                 std::vector<uint8_t> const& buf)
    {
        static const size_t runs = 256;
        std::vector<uint32_t> out_buf(mixed_block::block_size);

        // dry run to ignore one-time initializations (static variables, ...)
        mixed_block::decode(buf.data(), out_buf.data(),
                            sum_of_values, n);

        size_t spacing = 1 << 10;
        thread_local std::vector<uint8_t> readbuf(runs * spacing);
        thread_local std::vector<uint8_t const*> positions(runs);
        for (size_t run = 0; run < runs; ++run) {
            // try random alignments
            // XXX switch to c++ gens
            uint8_t* position = readbuf.data() + run * spacing + (rand() % 64);
            std::copy(buf.begin(), buf.end(), position);
            positions[run] = position;
        }

        double tick = get_time_usecs();
        for (auto position: positions) {
            mixed_block::decode(position, out_buf.data(), sum_of_values, n);
            do_not_optimize_away(out_buf[0]);
        }

        return (get_time_usecs() - tick) / runs * 1000;
    }

    void profile_block(std::vector<uint32_t> const& values,
                       uint32_t sum_of_values)
    {
        using namespace time_prediction;
        std::vector<uint8_t> buf;
        uint32_t n = values.size();
        feature_vector fv;
        values_statistics(values, fv);

        for (uint8_t t = 0; t < mixed_block::block_types; ++t) {
            mixed_block::block_type type = (mixed_block::block_type)t;
            for (mixed_block::compr_param_type param = 0;
                 param < mixed_block::compr_params(type); ++param) {
                buf.clear();
                if (!mixed_block::compression_stats(type, param, values.data(),
                                                    sum_of_values, n, buf, fv)) {
                    continue;
                }

                double time = measure_decoding_time(sum_of_values, n, buf);

                stats_line()
                    ("type", (int)t)
                    ("time", time)
                    (fv)
                    ;
            }
        }
    }

    template <typename IndexType>
    void profile_decoding(const char* index_filename,
                          double p)
    {
        std::default_random_engine rng(1729);
        std::uniform_real_distribution<double> dist01(0.0, 1.0);

        IndexType index;
        logger() << "Loading index from " << index_filename << std::endl;
        boost::iostreams::mapped_file_source m(index_filename);
        succinct::mapper::map(index, m);

        std::vector<uint32_t> values;

        for (size_t l = 0; l < index.size(); ++l) {
            if (l % 1000000 == 0) {
                logger() << l << " lists processed" << std::endl;
            }

            auto blocks = index[l].get_blocks();
            for (auto const& block: blocks) {
                // only measure full blocks
                if (block.size == mixed_block::block_size && dist01(rng) < p) {
                    block.decode_doc_gaps(values);
                    profile_block(values, block.doc_gaps_universe);
                    block.decode_freqs(values);
                    profile_block(values, uint32_t(-1));
                }
            }
        }

        logger() << index.size() << " lists processed" << std::endl;
    }
}

int main(int /* argc */, const char** argv)
{
    using namespace ds2i;

    std::string type = argv[1];
    const char* index_filename = argv[2];
    double p = boost::lexical_cast<double>(argv[3]);

    if (false) {
#define LOOP_BODY(R, DATA, T)                           \
        } else if (type == BOOST_PP_STRINGIZE(T)) {     \
            profile_decoding<BOOST_PP_CAT(T, _index)>   \
                (index_filename, p);                    \
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_BLOCK_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

}
