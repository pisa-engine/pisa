#include "create_wand_data.hpp"
#include "CLI/CLI.hpp"

int main(int argc, const char** argv)
{
    CLI::App app{"Creates additional data for query processing."};
    pisa::CreateWandDataArgs args(&app);
    CLI11_PARSE(app, argc, argv);
    pisa::create_wand_data(args);
}
