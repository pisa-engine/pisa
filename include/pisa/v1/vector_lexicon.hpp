#pragma once

#include <vector>

#include <gsl/span>
#include <gsl/gsl_assert>

#include "binary_freq_collection.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

struct VectorLexicon {
    explicit VectorLexicon(binary_freq_collection const &collection)
    {
        m_offsets.push_back(0);
        for (auto const &postings : collection) {
            m_offsets.push_back(postings.docs.size());
        }
    }

    [[nodiscard]] auto fetch(TermId term, gsl::span<std::byte const> bytes)
        -> gsl::span<std::byte const>
    {
        Expects(term + 1 < m_offsets.size());
        return bytes.subspan(m_offsets[term], m_offsets[term + 1]);
    }

   private:
    std::vector<std::size_t> m_offsets{};
};

} // namespace pisa::v1
