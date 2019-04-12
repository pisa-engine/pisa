#include <fstream>
#include <string>
#include <optional>

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
static std::string const DESC_ATT   = "Description:";
static std::string const DESC_END  = "</desc>";
static std::string const NARR       = "<narr>";
static std::string const NARR_ATT   = "Narrative:";
static std::string const NARR_END   = "</narr>";

std::string ltrim(const std::string &&str) {
    auto trimmed = str;
    auto it2     = std::find_if(trimmed.begin(), trimmed.end(), [](char ch) {
        return !std::isspace<char>(ch, std::locale::classic());
    });
    trimmed.erase(trimmed.begin(), it2);
    return trimmed;
}

std::string rtrim(const std::string &&str) {
    auto trimmed = str;
    auto it1     = std::find_if(trimmed.rbegin(), trimmed.rend(), [](char ch) {
        return !std::isspace<char>(ch, std::locale::classic());
    });
    trimmed.erase(it1.base(), trimmed.end());
    return trimmed;
}
std::string trim(const std::string &&str) { return ltrim(rtrim(std::forward<decltype(str)>(str))); }

struct Topic {
    std::string num;
    std::string title;
    std::string desc;
    std::string narr;
};

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

std::optional<Topic> next_topic(std::ifstream &is) {
    if(is.eof()) return std::nullopt;
    Topic              topic;
    std::ostringstream os;
    is >> std::ws;
    if(not detail::consume(is, TOP)){
        return std::nullopt;
    };
    detail::consume(is, NUM);
    detail::consume(is, NUM_ATT);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.num = trim(os.str());
    detail::consume(is, NUM_END);

    os.clear();
    detail::consume(is, TITLE);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.title = trim(os.str());
    detail::consume(is, TITLE_END);

    os.clear();
    detail::consume(is, DESC);
    detail::consume(is, DESC_ATT);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.desc = trim(os.str());
    detail::consume(is, DESC_END);

    os.clear();
    detail::consume(is, NARR);
    detail::consume(is, NARR_ATT);
    read_until(is, [](auto ch) { return ch == '<'; }, os);
    topic.narr = trim(os.str());
    detail::consume(is, NARR_END);

    detail::consume(is, TOP_END);

    return std::make_optional(topic);
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
    std::vector<detail::Topic> topics;
    while(auto topic = detail::next_topic(infile)){
        topics.push_back(topic.value());
    }

}