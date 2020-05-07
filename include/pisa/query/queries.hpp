#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace pisa {

using term_id_type = uint32_t;
using term_id_vec = std::vector<term_id_type>;
using term_freq_pair = std::pair<uint64_t, uint64_t>;
using term_freq_vec = std::vector<term_freq_pair>;

}  // namespace pisa
