#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "taily_thresholds.hpp"

int main(int argc, const char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    // set full precision for floats
    std::cout.precision(std::numeric_limits<float>::max_digits10);

    CLI::App app{"Estimates query thresholds using Taily cut-offs."};
    pisa::TailyThresholds args(&app);
    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(args.log_level());

    try {
        pisa::estimate_taily_thresholds(args);
    } catch (std::exception const& err) {
        spdlog::error("{}", err.what());
    } catch (...) {
        spdlog::error("Unknown error");
    }
}
