#include <iostream>
#include <sstream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "io.hpp"
#include "payload_vector.hpp"

namespace arg = pisa::arg;
using pisa::Payload_Vector;

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    pisa::App<arg::Index> app("Each ID from input translated to term");
    CLI11_PARSE(app, argc, argv);

    auto meta = app.index_metadata();
    if (not meta.term_lexicon) {
        spdlog::error("Term lexicon not defined");
        std::exit(1);
    }
    auto source = std::make_shared<mio::mmap_source>(meta.term_lexicon.value().c_str());
    auto lex = pisa::Payload_Vector<>::from(*source);

    pisa::io::for_each_line(std::cin, [&](auto&& line) {
        std::istringstream is(line);
        std::string next;
        if (is >> next) {
            std::cout << lex[std::stoi(next)];
        }
        while (is >> next) {
            std::cout << fmt::format(" {}", lex[std::stoi(next)]);
        }
        std::cout << '\n';
    });

    return 0;
}
