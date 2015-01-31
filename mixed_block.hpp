#pragma once

#include <string>

#include "block_codecs.hpp"
#include "dec_time_prediction.hpp"

namespace ds2i {

        struct mixed_block {

            enum class block_type : uint8_t{
                pfor = 0,
                varint = 1,
                interpolative = 2
            };

            typedef uint8_t compr_param_type;
            static compr_param_type compr_params(block_type t)
            {
                switch (t) {
                case block_type::pfor:
                    return optpfor_block::codec_type::possLogs.size();
                default: return 1;
                }
            }

            static const size_t block_types = 3;
            static const uint64_t block_size = 128;

            static void encode(uint32_t const*, uint32_t,
                               size_t, std::vector<uint8_t>&)
            {
                throw std::runtime_error("Mixed block indexes can only be created by transformation");
            }

            static void encode_type(block_type type, compr_param_type param,
                                    uint32_t const* in, uint32_t sum_of_values,
                                    size_t n, std::vector<uint8_t>& out)
            {
                assert(n <= block_size);
                if (n < block_size) {
                    if (type != block_type::interpolative) {
                        throw std::runtime_error("Partial blocks can only be encoded with interpolative");
                    }
                } else {
                    out.push_back((uint8_t)type);
                }

                switch (type) {
                case block_type::pfor: {
                    uint8_t b = optpfor_block::codec_type::possLogs[param];
                    optpfor_block::encode(in, sum_of_values, n, out, &b);
                    break;
                }
                case block_type::varint:
                    varint_G8IU_block::encode(in, sum_of_values, n, out);
                    break;
                case block_type::interpolative:
                    interpolative_block::encode(in, sum_of_values, n, out);
                    break;
                default:
                    throw std::runtime_error("Unsupported block type");
                }
            }

            static bool compression_stats(block_type type, compr_param_type param,
                                          uint32_t const* in, uint32_t sum_of_values,
                                          size_t n, std::vector<uint8_t>& buf,
                                          time_prediction::feature_vector& fv)
            {
                assert(buf.empty());
                using namespace time_prediction;

                if (n != block_size && type != block_type::interpolative) {
                    return false; // we use only interpolative for partial blocks
                }

                fv[feature_type::pfor_b] = 0;
                fv[feature_type::pfor_exceptions] = 0;

                // codec-specific stats
                if (type == block_type::pfor) {
                    auto const& possLogs = optpfor_block::codec_type::possLogs;
                    uint32_t b = possLogs[param];
                    uint32_t max_b = (uint32_t)fv[feature_type::max_b]; // float is exact up to 2^24
                    if (b > max_b && possLogs[param - 1] >= max_b) return false; // useless
                    if (max_b - b > 28) return false; // exception coder can't handle this
                    uint32_t exceptions = 0;
                    for (size_t i = 0; i < n; ++i) {
                        if (in[i] >= (uint32_t(1) << b)) {
                            exceptions += 1;
                        }
                    }
                    fv[feature_type::pfor_b] = b;
                    fv[feature_type::pfor_exceptions] = exceptions;
                }

                mixed_block::encode_type(type, param, in, sum_of_values, n, buf);
                fv[feature_type::size] = buf.size();

                return true;
            }

            struct space_time_point {
                float time;
                uint16_t space;
                block_type type;
                compr_param_type param;

                bool operator<(space_time_point const& other) const
                {
                    return std::make_pair(space, time) <
                        std::make_pair(other.space, other.time);
                }
            };

            static std::vector<space_time_point>
            compute_space_time(std::vector<uint32_t> const& values,
                               uint32_t sum_of_values,
                               std::vector<time_prediction::predictor> const& predictors,
                               uint32_t access_count)
            {
                using namespace time_prediction;
                std::vector<space_time_point> points;
                thread_local std::vector<uint8_t> buf;
                feature_vector fv;
                values_statistics(values, fv);

                for (uint8_t t = 0; t < block_types; ++t) {
                    block_type type = (block_type)t;
                    for (compr_param_type param = 0; param < compr_params(type); ++param) {
                        buf.clear();
                        if (!compression_stats(type, param, values.data(),
                                               sum_of_values, values.size(), buf, fv)) {
                            continue;
                        }

                        uint16_t space = (uint16_t)buf.size();
                        float time = 0;
                        if (values.size() == block_size) { // only predict time for full blocks
                            time = predictors[t](fv) * access_count;
                        }
                        points.push_back(space_time_point { time, space, block_type(type), param });
                    }
                }

                return points;
            }

            template <typename InputBlockData>
            struct block_transformer {
                block_transformer(InputBlockData input_block,
                                  block_type docs_type,
                                  block_type freqs_type,
                                  compr_param_type docs_param,
                                  compr_param_type freqs_param)
                    : index(input_block.index)
                    , max(input_block.max)
                    , size(input_block.size)
                    , doc_gaps_universe(input_block.doc_gaps_universe)
                    , m_input_block(input_block)
                    , m_docs_type(docs_type)
                    , m_freqs_type(freqs_type)
                    , m_docs_param(docs_param)
                    , m_freqs_param(freqs_param)
                {}

                uint32_t index;
                uint32_t max;
                uint32_t size;
                uint32_t doc_gaps_universe;

                void append_docs_block(std::vector<uint8_t>& out) const
                {
                    thread_local std::vector<uint32_t> buf;
                    m_input_block.decode_doc_gaps(buf);
                    encode_type(m_docs_type, m_docs_param, buf.data(),
                                doc_gaps_universe, size, out);
                }

                void append_freqs_block(std::vector<uint8_t>& out) const
                {
                    thread_local std::vector<uint32_t> buf;
                    m_input_block.decode_freqs(buf);
                    encode_type(m_freqs_type, m_freqs_param, buf.data(),
                                uint32_t(-1), size, out);
                }

            private:
                InputBlockData m_input_block;
                block_type m_docs_type, m_freqs_type;
                compr_param_type m_docs_param, m_freqs_param;
            };


            static uint8_t const* decode(uint8_t const* in, uint32_t* out,
                                         uint32_t sum_of_values, size_t n)
            {
                block_type type = block_type::interpolative;
                if (DS2I_LIKELY(n == block_size)) {
                    type = (block_type)*in++;
                }

                // use ifs instead of a switch to enable DS2I_LIKELY
                if (DS2I_LIKELY(type == block_type::varint)) { // optimize for the fastest codec
                    return varint_G8IU_block::decode(in, out, sum_of_values, n);
                } else if (type == block_type::pfor) {
                    return optpfor_block::decode(in, out, sum_of_values, n);
                } else if (type == block_type::interpolative) {
                    return interpolative_block::decode(in, out, sum_of_values, n);
                } else {
                    assert(false);
                    __builtin_unreachable();
                }
            }
        };

    typedef std::vector<ds2i::time_prediction::predictor> predictors_vec_type;

    predictors_vec_type load_predictors(const char* predictors_filename)
    {
        std::vector<time_prediction::predictor> predictors(mixed_block::block_types);

        std::ifstream fin(predictors_filename);

        std::string line;
        while (std::getline(fin, line)) {
            std::istringstream is(line);
            std::string field;
            size_t type;
            is >> field >> type;
            if (field != "type") { throw std::invalid_argument("Invalid input format"); }
            std::vector<std::pair<std::string, float>> values;
            while (true) {
                float value;
                if (!(is >> field >> value)) break;
                values.emplace_back(field, value);
            }

            if (type >= mixed_block::block_types) {
                throw std::invalid_argument("Invalid type while loading predictors");
            }
            predictors[type] = time_prediction::predictor(values);
        }

        return predictors;
    }

}
