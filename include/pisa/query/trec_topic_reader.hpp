#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>

namespace pisa {

namespace {

    static std::string const TOP = "<top>";
    static std::string const TOP_END = "</top>";
    static std::string const NUM = "<num>";
    static std::string const NUM_ATT = "Number:";
    static std::string const NUM_END = "</num>";
    static std::string const TITLE = "<title>";
    static std::string const TITLE_END = "</title>";
    static std::string const DESC = "<desc>";
    static std::string const DESC_ATT = "Description:";
    static std::string const DESC_END = "</desc>";
    static std::string const NARR = "<narr>";
    static std::string const NARR_ATT = "Narrative:";
    static std::string const NARR_END = "</narr>";

    static void consume(std::istream& is, std::string const& token, bool strict = true)
    {
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
    static std::ostream& read_until(std::istream& is, Pred pred, std::ostream& os)
    {
        is >> std::ws;
        while (not is.eof()) {
            if (is.peek() == std::istream::traits_type::eof() or pred(is.peek())) {
                break;
            }
            os.put(is.get());
        }
        return os;
    }

}  // namespace
struct trec_topic {
    std::string num;
    std::string title;
    std::string desc;
    std::string narr;
};

class trec_topic_reader {
  public:
    explicit trec_topic_reader(std::istream& is) : m_is(is) {}

    std::optional<trec_topic> next_topic()
    {
        m_is >> std::ws;
        if (m_is.eof()) {
            return std::nullopt;
        }

        trec_topic topic;
        std::ostringstream os;

        consume(m_is, TOP);

        consume(m_is, NUM);
        consume(m_is, NUM_ATT);
        read_until(
            m_is, [](auto ch) { return ch == '<'; }, os);
        topic.num = boost::algorithm::trim_copy(os.str());
        consume(m_is, NUM_END, false);

        os.str("");
        consume(m_is, TITLE);
        read_until(
            m_is, [](auto ch) { return ch == '<'; }, os);
        topic.title = boost::algorithm::trim_copy(os.str());
        boost::replace_all(topic.title, "\n", " ");
        consume(m_is, TITLE_END, false);

        os.str("");
        consume(m_is, DESC);
        consume(m_is, DESC_ATT, false);
        read_until(
            m_is, [](auto ch) { return ch == '<'; }, os);
        topic.desc = boost::algorithm::trim_copy(os.str());
        boost::replace_all(topic.desc, "\n", " ");
        consume(m_is, DESC_END, false);

        os.str("");
        consume(m_is, NARR);
        consume(m_is, NARR_ATT, false);
        read_until(
            m_is, [](auto ch) { return ch == '<'; }, os);
        topic.narr = boost::algorithm::trim_copy(os.str());
        boost::replace_all(topic.narr, "\n", " ");
        consume(m_is, NARR_END, false);

        consume(m_is, TOP_END);
        return std::make_optional(topic);
    }

  private:
    std::istream& m_is;
};

}  // namespace pisa
