#include <fstream>
#include <string>

#include "pisa/query/trec_topic_reader.hpp"

#include "CLI/CLI.hpp"

int main(int argc, char const *argv[]) {
    std::string input_filename;
    std::string output_basename;

    CLI::App app{"trec2query - a tool for converting TREC queries to PISA queries."};
    app.add_option("-i,--input", input_filename, "TREC query input file")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    CLI11_PARSE(app, argc, argv);

    std::ofstream title_file;
    title_file.open(output_basename + ".title");
    std::ofstream desc_file;
    desc_file.open(output_basename + ".desc");
    std::ofstream narr_file;
    narr_file.open(output_basename + ".narr");

    auto                    input_file = std::ifstream(input_filename);
    pisa::trec_topic_reader reader(input_file);

    while (auto topic = reader.next_topic()) {
        auto t = *topic;
        title_file << t.num << ":" << t.title << std::endl;
        desc_file << t.num << ":" << t.desc << std::endl;
        narr_file << t.num << ":" << t.narr << std::endl;
    }
}
