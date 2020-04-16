#pragma once

#include "app.hpp"
#include "pisa/binary_freq_collection.hpp"
#include "pisa/compress.hpp"
#include "pisa/global_parameters.hpp"

namespace pisa {

[[nodiscard]] auto compress_index(CompressArgs const& args) -> int
{
    binary_freq_collection input(args.input_basename().c_str());
    global_parameters params;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                                    \
    }                                                                            \
    else if (args.index_encoding() == BOOST_PP_STRINGIZE(T))                     \
    {                                                                            \
        compress_index<pisa::BOOST_PP_CAT(T, _index), wand_data<wand_data_raw>>( \
            input,                                                               \
            params,                                                              \
            args.output(),                                                       \
            args.check(),                                                        \
            args.index_encoding(),                                               \
            args.wand_data_path(),                                               \
            args.scorer(),                                                       \
            args.quantize());                                                    \
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", args.index_encoding());
        return 1;
    }
    return 0;
}

}  // namespace pisa
