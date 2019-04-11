#include <vector>

#include "CLI/CLI.hpp"


int main(int argc, char const *argv[])
{

    std::string input_filename;
    std::string output_filename;

    CLI::App app{"trec2query - a tool for converting TREC queries to PISA queries."};
    app.add_option("-i,--input", input_filename, "TREC query input file")->required();
    app.add_option("-o,--output", output_filename, "Query output file")->required();





    return 0;
}