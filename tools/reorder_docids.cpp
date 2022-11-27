#include <CLI/CLI.hpp>

#include "app.hpp"
#include "reorder_docids.hpp"

int main(int argc, const char** argv)
{
    CLI::App app{"Reassigns the document IDs."};
    pisa::ReorderDocuments args(&app);
    CLI11_PARSE(app, argc, argv);
    spdlog::set_level(args.log_level());
    pisa::reorder_docids(args);
}
