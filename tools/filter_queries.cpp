#include <iostream>

#include <CLI/CLI.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include "app.hpp"
#include "query.hpp"
#include "tokenizer.hpp"

namespace arg = pisa::arg;
using pisa::QueryContainer;
using pisa::io::for_each_line;

class TermProcessor {
  private:
    std::unordered_set<std::uint32_t> stopwords;

    std::function<std::optional<std::uint32_t>(std::string const&)> m_to_id;
    pisa::Stemmer_t m_stemmer;

  public:
    TermProcessor(
        std::optional<std::string> const& terms_file,
        std::optional<std::string> const& stopwords_filename,
        std::optional<std::string> const& stemmer_type)
    {
        auto source = std::make_shared<mio::mmap_source>(terms_file->c_str());
        auto terms = pisa::Payload_Vector<>::from(*source);

        m_to_id = [source = std::move(source), terms](auto str) -> std::optional<std::uint32_t> {
            // Note: the lexicographical order of the terms matters.
            auto pos = std::lower_bound(terms.begin(), terms.end(), std::string_view(str));
            if (*pos == std::string_view(str)) {
                return std::distance(terms.begin(), pos);
            }
            return std::nullopt;
        };

        m_stemmer = pisa::term_processor(stemmer_type);

        if (stopwords_filename) {
            std::ifstream is(*stopwords_filename);
            pisa::io::for_each_line(is, [&](auto&& word) {
                if (auto processed_term = m_to_id(std::move(word)); processed_term.has_value()) {
                    stopwords.insert(*processed_term);
                }
            });
        }
    }

    [[nodiscard]] std::optional<pisa::ParsedTerm> operator()(std::string token)
    {
        token = m_stemmer(token);
        auto id = m_to_id(token);
        if (not id) {
            return std::nullopt;
        }
        if (is_stopword(*id)) {
            return std::nullopt;
        }
        return pisa::ParsedTerm{*id, token};
    }

    [[nodiscard]] auto is_stopword(std::uint32_t const term) const -> bool
    {
        return stopwords.find(term) != stopwords.end();
    }
};

enum class Format { Json, Colon };

void filter_queries(
    std::optional<std::string> const& query_file,
    std::optional<std::string> const& term_lexicon,
    std::optional<std::string> const& stemmer,
    std::optional<std::string> const& stopwords_filename,
    std::size_t min_query_len,
    std::size_t max_query_len)
{
    std::optional<Format> fmt{};
    auto parser = [term_processor = TermProcessor(term_lexicon, stopwords_filename, stemmer)](
                      auto query) mutable {
        std::vector<pisa::ParsedTerm> parsed_terms;
        pisa::TermTokenizer tokenizer(query);
        for (auto term_iter = tokenizer.begin(); term_iter != tokenizer.end(); ++term_iter) {
            auto term = term_processor(*term_iter);
            if (term) {
                parsed_terms.push_back(std::move(*term));
            }
        }
        return parsed_terms;
    };
    auto filter = [&](auto&& line) {
        auto query = [&] {
            if (fmt) {
                if (*fmt == Format::Json) {
                    return QueryContainer::from_json(line);
                }
                return QueryContainer::from_colon_format(line);
            }
            try {
                auto query = QueryContainer::from_json(line);
                fmt = Format::Json;
                return query;
            } catch (std::exception const& err) {
                fmt = Format::Colon;
                return QueryContainer::from_colon_format(line);
            }
        }();
        query.parse(parser);
        if (auto len = query.term_ids()->size(); len >= min_query_len && len <= max_query_len) {
            std::cout << query.to_json() << '\n';
        }
    };
    if (query_file) {
        std::ifstream is(*query_file);
        for_each_line(is, filter);
    } else {
        for_each_line(std::cin, filter);
    }
}

int main(int argc, char** argv)
{
    spdlog::drop("");
    spdlog::set_default_logger(spdlog::stderr_color_mt(""));

    std::size_t min_query_len = 1;
    std::size_t max_query_len = std::numeric_limits<std::size_t>::max();

    pisa::App<arg::Query<arg::QueryMode::Unranked>> app(
        "Filters out empty queries against a v1 index.");
    app.add_option("--min", min_query_len, "Minimum query legth to consider");
    app.add_option("--max", max_query_len, "Maximum query legth to consider");
    CLI11_PARSE(app, argc, argv);

    filter_queries(
        app.query_file(),
        app.term_lexicon(),
        app.stemmer(),
        app.stop_words(),
        min_query_len,
        max_query_len);
    return 0;
}
