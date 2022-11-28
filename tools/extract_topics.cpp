#include <fstream>
#include <string>

#include <spdlog/spdlog.h>

#include "app.hpp"
#include "pisa/query/aol_reader.hpp"
#include "pisa/query/trec_topic_reader.hpp"

#include "CLI/CLI.hpp"

int main(int argc, char const* argv[])
{
    std::string input_filename;
    std::string output_basename;
    std::string format;
    bool unique = false;

    pisa::App<pisa::arg::LogLevel> app{
        "A tool for converting queries from several formats to PISA queries."};
    app.add_option("-i,--input", input_filename, "TREC query input file")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    app.add_option("-f,--format", format, "Input format")->required();
    app.add_flag("-u,--unique", unique, "Unique queries");

    CLI11_PARSE(app, argc, argv);

    spdlog::set_level(app.log_level());

    if (format == "trec") {
        std::ofstream title_file;
        title_file.open(output_basename + ".title");
        std::ofstream desc_file;
        desc_file.open(output_basename + ".desc");
        std::ofstream narr_file;
        narr_file.open(output_basename + ".narr");

        auto input_file = std::ifstream(input_filename);
        pisa::trec_topic_reader reader(input_file);

        while (auto topic = reader.next_topic()) {
            auto t = *topic;
            title_file << t.num << ":" << t.title << std::endl;
            desc_file << t.num << ":" << t.desc << std::endl;
            narr_file << t.num << ":" << t.narr << std::endl;
        }
    } else if (format == "aol") {
        std::ofstream query_file;
        query_file.open(output_basename + ".query");
        auto input_file = std::ifstream(input_filename);
        pisa::aol_reader reader(input_file);
        std::set<std::string> unique_queries;
        size_t id = 0;
        while (auto query = reader.next_query()) {
            query_file << id << ":" << *query << std::endl;
            unique_queries.insert(*query);
            id += 1;
        }
        if (unique) {
            std::ofstream unique_query_file;
            unique_query_file.open(output_basename + ".query.unique");
            size_t id = 0;
            for (auto&& uq: unique_queries) {
                unique_query_file << id << ":" << uq << std::endl;
                id += 1;
            }
        }
    } else {
        spdlog::error("Unsupported input format: {}", format);
        std::abort();
    }
}
