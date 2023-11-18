#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "compress.hpp"

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
        args.quantization_bits(),
        args.check());
}
