#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

using wand_raw_index = wand_data<wand_data_raw>;
using wand_uniform_index = wand_data<wand_data_compressed>;

template <typename IndexType, typename WandType>
void kth_scorer(const std::string &index_filename,
                const std::string &wand_data_filename,
                std::string const &scorer_name,
                size_t k)
{
}

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string index_filename;
    std::string wand_data_filename;
    std::string scorer_name;

    size_t k;
    bool compressed = false;

    CLI::App app{"A tool for storing and retrieving the k-th score of a term."};
    app.add_option("-i,--index", index_filename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename");
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_flag("--compressed-wand", compressed, "Compressed wand input file");
    app.add_option("-k", k, "k value");
    CLI11_PARSE(app, argc, argv);

    if (compressed) {
        kth_scorer<wand_uniform_index>(index_filename, wand_data_filename, scorer_name, k);
    } else {
        kth_scorer<wand_raw_index>(index_filename, wand_data_filename, scorer_name, k);
    }
}