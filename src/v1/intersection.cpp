#include <fstream>
#include <sstream>

#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>
#include <spdlog/spdlog.h>

#include "io.hpp"
#include "v1/intersection.hpp"

namespace pisa::v1 {

auto read_intersections(std::string const& filename) -> std::vector<std::vector<std::bitset<64>>>
{
    std::ifstream is(filename);
    return read_intersections(is);
}

auto read_intersections(std::istream& is) -> std::vector<std::vector<std::bitset<64>>>
{
    std::vector<std::vector<std::bitset<64>>> intersections;
    ::pisa::io::for_each_line(is, [&](auto const& query_line) {
        intersections.emplace_back();
        std::istringstream iss(query_line);
        std::transform(
            std::istream_iterator<std::string>(iss),
            std::istream_iterator<std::string>(),
            std::back_inserter(intersections.back()),
            [&](auto const& n) {
                auto bits = std::bitset<64>(std::stoul(n));
                if (bits.count() > 2) {
                    spdlog::error("Intersections of more than 2 terms not supported yet!");
                    std::exit(1);
                }
                return bits;
            });
    });
    return intersections;
}

[[nodiscard]] auto to_vector(std::bitset<64> const& bits) -> std::vector<std::size_t>
{
    std::vector<std::size_t> vec;
    for (auto idx = 0; idx < bits.size(); idx += 1) {
        if (bits.test(idx)) {
            vec.push_back(idx);
        }
    }
    return vec;
}

[[nodiscard]] auto filter_unigrams(std::vector<std::vector<std::bitset<64>>> const& intersections)
    -> std::vector<std::vector<std::size_t>>
{
    return intersections | ranges::views::transform([&](auto&& query_intersections) {
               return query_intersections | ranges::views::filter(is_n_gram(1))
                      | ranges::views::transform([&](auto bits) { return to_vector(bits)[0]; })
                      | ranges::to_vector;
           })
           | ranges::to_vector;
}

[[nodiscard]] auto filter_bigrams(std::vector<std::vector<std::bitset<64>>> const& intersections)
    -> std::vector<std::vector<std::pair<std::size_t, std::size_t>>>
{
    return intersections | ranges::views::transform([&](auto&& query_intersections) {
               return query_intersections | ranges::views::filter(is_n_gram(2))
                      | ranges::views::transform([&](auto bits) {
                            auto vec = to_vector(bits);
                            return std::make_pair(vec[0], vec[1]);
                        })
                      | ranges::to_vector;
           })
           | ranges::to_vector;
}

} // namespace pisa::v1
