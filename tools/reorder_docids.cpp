#include <CLI/CLI.hpp>

#include "app.hpp"
#include "reorder_docids.hpp"

int main(int argc, const char** argv)
{
    CLI::App app{"Reassigns the document IDs."};
    pisa::ReorderDocuments args(&app);
    CLI11_PARSE(app, argc, argv);
    pisa::reorder_docids(args);
}
