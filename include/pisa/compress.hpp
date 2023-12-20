#pragma once

#include <optional>
#include <string>

#include "scorer/scorer.hpp"
#include "type_safe.hpp"

namespace pisa {

void compress(
    std::string const& input_basename,
    std::optional<std::string> const& wand_data_filename,
    std::string const& index_encoding,
    std::string const& output_filename,
    ScorerParams const& scorer_params,
    std::optional<Size> quantization_bits,
    bool check
);

}  // namespace pisa
