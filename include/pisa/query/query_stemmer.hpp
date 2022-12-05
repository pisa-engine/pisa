#pragma once
#include <optional>
#include <sstream>
#include <string>

#include <boost/algorithm/string/join.hpp>

#include "query/queries.hpp"
#include "query/term_processor.hpp"
#include "tokenizer.hpp"

namespace pisa {

class QueryStemmer {
  public:
    explicit QueryStemmer(std::optional<std::string> const& stemmer_name)
        : m_stemmer(term_transformer_builder(stemmer_name)())
    {}
    std::string operator()(std::string const& query_string)
    {
        std::stringstream tokenized_query;
        auto [id, raw_query] = split_query_at_colon(query_string);
        std::vector<std::string> stemmed_terms;
        EnglishTokenStream tokenizer(raw_query);
        for (auto token: tokenizer) {
            stemmed_terms.push_back(m_stemmer(token));
        }
        if (id) {
            tokenized_query << *(id) << ":";
        }
        using boost::algorithm::join;
        tokenized_query << join(stemmed_terms, " ");
        return tokenized_query.str();
    }

    TermTransformer m_stemmer;
};

}  // namespace pisa
