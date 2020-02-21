#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace pisa {

using term_id_type = std::uint32_t;
using term_id_vec = std::vector<term_id_type>;

struct Query {
    std::optional<std::string> id;
    std::vector<term_id_type> terms;
    std::vector<float> term_weights;
};

} // namespace pisa
