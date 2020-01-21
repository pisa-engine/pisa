#include <CLI/CLI.hpp>
#include <boost/numeric/ublas/vector_sparse.hpp>
#include <optional>
#include <mio/mmap.hpp>

#include "binary_freq_collection.hpp"
#include "util/progress.hpp"
#include "mappable/mapper.hpp"
#include "wand_data.hpp"
#include "wand_data_raw.hpp"
#include "scorer/scorer.hpp"

using namespace pisa;
using namespace boost::numeric::ublas;

using wand_raw_index = wand_data<wand_data_raw>;
using id_type = std::uint32_t;

std::vector<compressed_vector<float>> from_inverted_index(const std::string &input_basename,
                                                          const std::string &wand_data_filename,
                                                          std::string const &scorer_name,
                                                          size_t             min_len)
{
    binary_freq_collection coll((input_basename).c_str());

    wand_raw_index wdata;
    mio::mmap_source md;
    std::error_code error;
    md.map(wand_data_filename, error);
    if (error) {
        std::cerr << "error mapping file: " << error.message() << ", exiting..." << std::endl;
        throw std::runtime_error("Error opening file");
    }
    mapper::map(wdata, md, mapper::map_flags::warmup);

    auto scorer = scorer::from_name(scorer_name, wdata);

    auto num_terms = 0;
    for (auto const &seq : coll) {
        if(seq.docs.size() >= min_len){
            num_terms += 1;
        }
    }
    spdlog::info("Number of terms: {}", num_terms);

    std::vector<compressed_vector<float>> fwd(coll.num_docs());
    for (auto &&d : fwd) {
        d.resize(num_terms);
    }

    {
        progress p("Building forward index", num_terms);
        id_type tid = 0;
        for (auto const &seq : coll) {
            auto t_scorer = scorer->term_scorer(tid);
            if(seq.docs.size() >= min_len){
                for (size_t i = 0; i < seq.docs.size(); ++i) {
                    uint64_t docid = *(seq.docs.begin() + i);
                    uint64_t freq = *(seq.freqs.begin() + i);
                    fwd[docid][tid] = t_scorer(docid, freq);
                }
                p.update(1);
                ++tid;
            }
        }
    }
    return fwd;
}

int main(int argc, char const *argv[])
{
    std::string input_basename;
    std::string wand_data_filename;
    std::string output_basename;
    std::optional<std::string> documents_filename;
    std::optional<std::string> reordered_documents_filename;
    size_t min_len = 0;

    CLI::App app{"K-means reordering algorithm used for inverted indexed reordering."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-w,--wand", wand_data_filename, "WAND data filename")->required();
    app.add_option("-o,--output", output_basename, "Output basename");
    app.add_option("-m,--min-len", min_len, "Minimum list threshold");
    auto docs_opt = app.add_option("--documents", documents_filename, "Documents lexicon");
    app.add_option(
           "--reordered-documents", reordered_documents_filename, "Reordered documents lexicon")
        ->needs(docs_opt);
    CLI11_PARSE(app, argc, argv);
    auto fwd = from_inverted_index(input_basename, wand_data_filename, "bm25", min_len);



}