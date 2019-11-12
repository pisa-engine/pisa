#pragma once

#include <cstddef>
#include <fstream>
#include <string>
#include <vector>

namespace pisa::v1 {

[[nodiscard]] auto load_bytes(std::string const &data_file) -> std::vector<std::byte>;

} // namespace pisa::v1
