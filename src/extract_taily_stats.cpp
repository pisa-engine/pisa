#include <iostream>
#include <optional>

#include "scorer/scorer.hpp"
#include "mio/mmap.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

int main(int argc, const char **argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::string input_basename;
    std::string scorer_name;
    std::string wand_data_filename;

    std::string output_filename;

    CLI::App app{"A tool for extracting Taily statistics on an index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "Wand data filename")->required();
    app.add_option("-s,--scorer", scorer_name, "Scorer function")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    CLI11_PARSE(app, argc, argv);

    mio::mmap_source md;
    if (wand_data_filename) {
        std::error_code error;
        md.map(*wand_data_filename, error);
        if (error) {
            spdlog::error("error mapping file: {}, exiting...", error.message());
            std::abort();
        }
        mapper::map(wdata, md, mapper::map_flags::warmup);
    }

    auto scorer = scorer::from_name(scorer_name, *this);

    binary_freq_collection coll(input_basename.c_str());
    size_t term_id = 0;
    for (auto const &seq : coll) {
        auto size = seq.docs.size();
        auto term_scorer = scorer->term_scorer(term_id);
        for (i = 0; i < seq.docs.size(); ++i) {
            uint64_t docid = *(seq.docs.begin() + i);
            uint64_t freq = *(seq.freqs.begin() + i);
            float score = term_scorer(docid, freq);
        }
        term_id += 1;
    }
}
