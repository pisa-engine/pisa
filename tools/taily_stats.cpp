#include <iostream>
#include <optional>

#include <CLI/CLI.hpp>
#include <mio/mmap.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <taily.hpp>

#include "./taily_stats.hpp"
#include "app.hpp"
#include "binary_freq_collection.hpp"
#include "memory_source.hpp"
#include "scorer/scorer.hpp"
#include "util/progress.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

namespace arg = pisa::arg;
using pisa::Args;
using pisa::wand_data;
using pisa::wand_data_compressed;
using pisa::wand_data_raw;

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    CLI::App app{"Extracts Taily statistics from the index and stores it in a file."};
    pisa::TailyStatsArgs args(&app);
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(args.log_level());

    try {
        pisa::extract_taily_stats(args);
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
    } catch (...) {
        spdlog::error("Unknown error occurred.");
    }
}
