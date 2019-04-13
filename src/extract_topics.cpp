#include <fstream>
#include <optional>
#include <string>

#include "pisa/util/string_util.hpp"

#include "CLI/CLI.hpp"

namespace detail {

static std::string const TOP       = "<top>";
static std::string const TOP_END   = "</top>";
static std::string const NUM       = "<num>";
static std::string const NUM_ATT   = "Number:";
static std::string const NUM_END   = "</num>";
static std::string const TITLE     = "<title>";
static std::string const TITLE_END = "</title>";
static std::string const DESC      = "<desc>";
static std::string const DESC_ATT  = "Description:";
static std::string const DESC_END  = "</desc>";
static std::string const NARR      = "<narr>";
static std::string const NARR_ATT  = "Narrative:";
static std::string const NARR_END  = "</narr>";

struct Topic {
    std::string num;
    std::string title;
    std::string desc;
    std::string narr;
};

void consume(std::istream &is, std::string const &token, bool strict = true) {
    is >> std::ws;
    for (auto pos = token.begin(); pos != token.end(); ++pos) {
        if (is.get() != *pos) {
            is.unget();
            for (auto rpos = std::reverse_iterator(pos); rpos != token.rend(); ++rpos) {
                is.putback(*rpos);
            }
            if (strict) {
                throw std::runtime_error("Could not consume tag: " + token);
            }
            break;
        }
    }
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

std::optional<Topic> next_topic(std::ifstream &is) {
    is >> std::ws;
    if (is.eof()) return std::nullopt;

    Topic              topic;
    std::ostringstream os;

    detail::consume(is, TOP);

    detail::consume(is, NUM);
    detail::consume(is, NUM_ATT);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.num = pisa::util::trim(os.str());
    detail::consume(is, NUM_END, false);

    os.str("");
    detail::consume(is, TITLE);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.title = pisa::util::trim(os.str());
    detail::consume(is, TITLE_END, false);

    os.str("");
    detail::consume(is, DESC);
    detail::consume(is, DESC_ATT, false);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.desc = pisa::util::trim(os.str());
    detail::consume(is, DESC_END, false);

    os.str("");
    detail::consume(is, NARR);
    detail::consume(is, NARR_ATT, false);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.narr = pisa::util::trim(os.str());
    detail::consume(is, NARR_END, false);

    detail::consume(is, TOP_END);
    return std::make_optional(topic);
}

}  // namespace detail

int main(int argc, char const *argv[]) {
    std::string input_filename;
    std::string output_basename;

    CLI::App app{"trec2query - a tool for converting TREC queries to PISA queries."};
    app.add_option("-i,--input", input_filename, "TREC query input file")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    CLI11_PARSE(app, argc, argv);

    std::ifstream infile(input_filename);
    std::ofstream title_file;
    title_file.open(output_basename + ".title");
    std::ofstream desc_file;
    desc_file.open(output_basename + ".desc");
    std::ofstream narr_file;
    narr_file.open(output_basename + ".narr");

    while (auto topic = detail::next_topic(infile)) {
        auto t = topic.value();
        title_file << t.num << ":" << t.title << std::endl;
        desc_file << t.num << ":" << t.desc << std::endl;
        narr_file << t.num << ":" << t.narr << std::endl;
    }
}