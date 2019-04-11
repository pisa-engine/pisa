#include <fstream>
#include <string>

#include "CLI/CLI.hpp"
namespace detail {

static std::string const TOP       = "<top>";
static std::string const TOP_END   = "</top>";
static std::string const NUM       = "<num>";
static std::string const NUM_END   = "</num>";
static std::string const TITLE     = "<title>";
static std::string const TITLE_END = "</title>";
static std::string const DESC      = "<desc>";
static std::string const DESC_END  = "</desc>";
static std::string const NAR       = "<narr>";
static std::string const NAR_END   = "</narr>";

bool consume(std::istream &is, std::string const &token) {
    is >> std::ws;
    for (auto pos = token.begin(); pos != token.end(); ++pos) {
        if (is.get() != *pos) {
            is.unget();
            for (auto rpos = std::reverse_iterator(pos); rpos != token.rend(); ++rpos) {
                is.putback(*rpos);
            }
            return false;
        }
    }
    return true;
}

template <typename Pred>
std::ostream &read_until(std::istream &is, Pred pred, std::ostream &os) {
    is >> std::ws;
    while (not is.eof()) {
        if (is.peek() == std::istream::traits_type::eof() or pred(is.peek())) {
            break;
        }
        os.put(is.get());
    }
    return os;
}

void next_record(std::ifstream &is) {
    std::ostringstream os;
    is >> std::ws;

    detail::consume(is, TOP);
    detail::consume(is, NUM);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    detail::consume(is, TITLE);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    detail::consume(is, DESC);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    detail::consume(is, NAR);
    read_until(is, [](auto ch) { return ch == '<'; }, os);

    while (read_until(is, [](auto ch) { return ch == '<'; }, os)) {
        if (is.peek() == std::istream::traits_type::eof()) {
            break;
        }
        if (consume(is, TOP_END)) {
            break;
        }
        os.put(is.get());
    }
    std::cout << os.str() << std::endl;
}

}  // namespace detail

int main(int argc, char const *argv[]) {
    std::string input_filename;
    std::string output_basename;

    CLI::App app{"trec2query - a tool for converting TREC queries to PISA queries."};
    app.add_option("-i,--input", input_filename, "TREC query input file")->required();
    app.add_option("-o,--output", output_basename, "Query output file")->required();
    CLI11_PARSE(app, argc, argv);

    std::ifstream infile(input_filename);
    detail::next_record(infile);
}