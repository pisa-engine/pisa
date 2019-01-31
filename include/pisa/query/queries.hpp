#pragma once

#include <iostream>
#include <sstream>

#include "index_types.hpp"
#include "topk_queue.hpp"
#include "util/util.hpp"
#include "wand_data.hpp"
#include "wand_data_compressed.hpp"
#include "wand_data_raw.hpp"

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;

inline bool read_query(term_id_vec &ret, std::istream &is = std::cin)
{
    ret.clear();
    std::string line;
    if (not std::getline(is, line)) {
        return false;
    }
    std::istringstream iline(line);
    term_id_type term_id;
    while (iline >> term_id) {
        ret.push_back(term_id);
    }
    return true;
}

inline void remove_duplicate_terms(term_id_vec &terms)
{
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

using term_freq_pair = std::pair<uint64_t, uint64_t>;
using term_freq_vec = std::vector<term_freq_pair>;

inline term_freq_vec query_freqs(term_id_vec terms)
{
    term_freq_vec query_term_freqs;
    std::sort(terms.begin(), terms.end());
    // count query term frequencies
    for (size_t i = 0; i < terms.size(); ++i) {
        if (i == 0 || terms[i] != terms[i - 1]) {
            query_term_freqs.emplace_back(terms[i], 1);
        } else {
            query_term_freqs.back().second += 1;
        }
    }

    return query_term_freqs;
}

template <typename Scorer, typename Wand>
struct Score_Function {
    float query_weight;
    std::reference_wrapper<Wand const> wdata;

    [[nodiscard]] auto operator()(uint32_t doc, uint32_t freq) const -> float {
        return query_weight * Scorer::doc_term_weight(freq, wdata.get().norm_len(doc));
    }
};

// TODO: These are functions common to query processing in general.
//       They should be moved out of this file.
namespace query {

template <typename Index, typename WandType>
[[nodiscard]] auto cursors_with_scores(Index const &index, WandType const &wdata, term_id_vec terms)
{
    // TODO(michal): parametrize scorer_type; didn't do that because this might mean some more
    //               complex refactoring I want to avoid for now.
    using scorer_type         = bm25;
    using cursor_type         = typename Index::document_enumerator;
    using score_function_type = Score_Function<scorer_type, WandType>;

    auto query_term_freqs = query_freqs(std::move(terms));
    std::vector<cursor_type> cursors;
    std::vector<score_function_type> score_functions;
    cursors.reserve(query_term_freqs.size());
    score_functions.reserve(query_term_freqs.size());

    for (auto term : query_term_freqs) {
        auto     list     = index[term.first];
        uint64_t num_docs = index.num_docs();
        auto     q_weight = scorer_type::query_term_weight(term.second, list.size(), num_docs);
        cursors.push_back(std::move(list));
        score_functions.push_back({q_weight, std::cref(wdata)});
    }
    return std::make_pair(cursors, score_functions);
}

} // namespace query
} // namespace pisa

#include "algorithm/and_query.hpp"
#include "algorithm/block_max_maxscore_query.hpp"
#include "algorithm/block_max_wand_query.hpp"
#include "algorithm/maxscore_query.hpp"
#include "algorithm/or_query.hpp"
#include "algorithm/ranked_and_query.hpp"
#include "algorithm/ranked_or_query.hpp"
#include "algorithm/wand_query.hpp"
#include "algorithm/ranked_or_taat_query.hpp"
