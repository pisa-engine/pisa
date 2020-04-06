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
#include "create_freq_index.hpp"
#include "index_types.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));
    CLI::App app{"Compresses an inverted index"};
    pisa::CompressArgs args(&app);
    CLI11_PARSE(app, argc, argv);
    return pisa::compress_index(args);
}
