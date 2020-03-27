#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "compress.hpp"
#include "index_types.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using wand_raw_index = pisa::wand_data<pisa::wand_data_raw>;
namespace arg = pisa::arg;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string input_basename;
    std::optional<std::string> output_filename;
    bool check = false;

    pisa::App<arg::Encoding, arg::Quantize> app{"Compresses an inverted index"};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    app.add_flag("--check", check, "Check the correctness of the index");
    CLI11_PARSE(app, argc, argv);

    pisa::binary_freq_collection input(input_basename.c_str());
    pisa::global_parameters params;

    if (false) {
#define LOOP_BODY(R, DATA, T)                                                \
    }                                                                        \
    else if (app.index_encoding() == BOOST_PP_STRINGIZE(T))                  \
    {                                                                        \
        pisa::compress_index<pisa::BOOST_PP_CAT(T, _index), wand_raw_index>( \
            input,                                                           \
            params,                                                          \
            output_filename,                                                 \
            check,                                                           \
            app.index_encoding(),                                            \
            app.wand_data_path(),                                            \
            app.scorer(),                                                    \
            app.quantize());                                                 \
        /**/
        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, PISA_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        spdlog::error("Unknown type {}", app.index_encoding());
    }

    return 0;
}
