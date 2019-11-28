#pragma once

#include <bitset>
#include <iostream>
#include <optional>
#include <vector>

namespace pisa::v1 {

/// Read a list of intersections.
///
/// Each line in the format relates to one query, and each space-separated value
/// is an integer intersection representation. These numbers are converted to
/// bitsets, and each 1 at position `i` means that the `i`-th term in the query
/// is present in the intersection.
///
/// # Example
///
/// Let `q = a b c d e` be our query. The following line:
/// ```
/// 1 2 5 16
/// ```
/// can be represented as bitsets:
/// ```
/// 00001 00010 00101 10000
/// ```
/// which in turn represent four intersection: a, b, ac, e.
[[nodiscard]] auto read_intersections(std::string const& filename)
    -> std::vector<std::vector<std::bitset<64>>>;
[[nodiscard]] auto read_intersections(std::istream& is)
    -> std::vector<std::vector<std::bitset<64>>>;

/// Converts a bitset to a vector of positions set to 1.
[[nodiscard]] auto to_vector(std::bitset<64> const& bits) -> std::vector<std::size_t>;

/// Returns a lambda taking a bitset and returning `true` if it has `n` set bits.
[[nodiscard]] inline auto is_n_gram(std::size_t n)
{
    return [n](std::bitset<64> const& bits) -> bool { return bits.count() == n; };
}

/// Returns only positions of terms in unigrams.
[[nodiscard]] auto filter_unigrams(std::vector<std::vector<std::bitset<64>>> const& intersections)
    -> std::vector<std::vector<std::size_t>>;

/// Returns only positions of terms in bigrams.
[[nodiscard]] auto filter_bigrams(std::vector<std::vector<std::bitset<64>>> const& intersections)
    -> std::vector<std::vector<std::pair<std::size_t, std::size_t>>>;

} // namespace pisa::v1
