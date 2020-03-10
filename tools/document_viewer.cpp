#include <CLI/CLI.hpp>
#include <mio/mmap.hpp>
#include <spdlog/spdlog.h>

#include "binary_collection.hpp"
#include "payload_vector.hpp"

using namespace pisa;

int main(int argc, char** argv)
{
    std::string fwd_filename;
    std::string terms_filename;
    std::string documents_filename;
    std::string document_name;

    CLI::App app{"Document viewer"};
    app.add_option("--fwd", fwd_filename, "Forward index filename")->required();
    app.add_option("--terms", terms_filename, "Terms filename")->required();
    app.add_option("--documents", documents_filename, "Terms filename")->required();
    app.add_option("--doc,-d", document_name, "Document name")->required();
    CLI11_PARSE(app, argc, argv);

    mio::mmap_source terms_m(terms_filename.c_str());
    auto lexicon = Payload_Vector<>::from(terms_m);

    mio::mmap_source documents_m(documents_filename.c_str());
    auto documents = Payload_Vector<>::from(documents_m);

    using document_id_t = uint32_t;
    auto doc_to_id = [documents = std::move(documents)](auto str) -> std::optional<document_id_t>{
        auto pos = std::find(documents.begin(), documents.end(), std::string_view(str));
        if (pos != documents.end()) {
            return std::distance(documents.begin(), pos);
        }
        return std::nullopt;
    };

    auto doc_id = doc_to_id(document_name);
    if (doc_id) {
        spdlog::info("Document {} has id equal to {}.", document_name, *doc_id);
    } else {
        spdlog::info("Document {} not found.", document_name);
        std::abort();
    }

    binary_collection fwd(fwd_filename.c_str());
    auto doc_iter = ++fwd.begin();
    for (int i = 0; i < (*doc_id); ++i)
    {
        ++doc_iter;
    }
    auto document_sequence = *(doc_iter);
    for(auto&& term_id : document_sequence) {
        std::cout << lexicon[term_id] << " ";
    }
    std::cout << std::endl;


}
