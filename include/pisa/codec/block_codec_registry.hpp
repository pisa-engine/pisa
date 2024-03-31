#pragma once

#include <memory>
#include <string_view>

#include <gsl/span>

#include "codec/block_codec.hpp"

namespace pisa {

[[nodiscard]] auto get_block_codec(std::string_view name) -> std::unique_ptr<BlockCodec>;
[[nodiscard]] auto get_block_codec_names() -> gsl::span<std::string_view const>;

}  // namespace pisa
