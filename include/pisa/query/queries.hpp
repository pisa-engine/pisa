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
typedef uint32_t                  term_id_type;
typedef std::vector<term_id_type> term_id_vec;

bool read_query(term_id_vec &ret, std::istream &is = std::cin) {
    ret.clear();
    std::string line;
    if (!std::getline(is, line))
        return false;
    std::istringstream iline(line);
    term_id_type       term_id;
    while (iline >> term_id) {
        ret.push_back(term_id);
    }

    return true;
}

void remove_duplicate_terms(term_id_vec &terms) {
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
}

typedef std::pair<uint64_t, uint64_t> term_freq_pair;
typedef std::vector<term_freq_pair>   term_freq_vec;

term_freq_vec query_freqs(term_id_vec terms) {
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

} // namespace pisa

#include "algorithm/and_query.hpp"
#include "algorithm/block_max_maxscore_query.hpp"
#include "algorithm/block_max_wand_query.hpp"
#include "algorithm/maxscore_query.hpp"
#include "algorithm/or_query.hpp"
#include "algorithm/ranked_and_query.hpp"
#include "algorithm/ranked_or_query.hpp"
#include "algorithm/wand_query.hpp"