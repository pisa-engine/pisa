#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include <boost/algorithm/string/predicate.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "CLI/CLI.hpp"
#include "app.hpp"
#include "compress.hpp"
#include "index_types.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));
    CLI::App app{"Compresses an inverted index"};
    pisa::CompressArgs args(&app);
    CLI11_PARSE(app, argc, argv);
    spdlog::set_level(args.log_level());
    pisa::compress(
        args.input_basename(),
        args.wand_data_path(),
        args.index_encoding(),
        args.output(),
        args.scorer_params(),
        args.quantize(),
        args.check());
}
