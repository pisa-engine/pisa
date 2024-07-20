#include "codec/block_codec_registry.hpp"

#include <array>
#include <string_view>

#include <fmt/format.h>
#include <gsl/span>

#include "codec/block_codec.hpp"
#include "codec/interpolative.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/optpfor.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varint_g8iu.hpp"
#include "codec/varintgb.hpp"

namespace pisa {

using BlockCodecs = BlockCodecRegistry<
    InterpolativeBlockCodec,
    MaskedVByteBlockCodec,
    OptPForBlockCodec,
    QmxBlockCodec,
    SimdBpBlockCodec,
    Simple16BlockCodec,
    Simple8bBlockCodec,
    StreamVByteBlockCodec,
    VarintG8IUBlockCodec,
    VarintGbBlockCodec>;

auto get_block_codec(std::string_view name) -> BlockCodecPtr {
    return BlockCodecs::get(name);
}

constexpr auto get_block_codec_names() -> gsl::span<std::string_view const> {
    return gsl::make_span<std::string_view const>(&BlockCodecs::names[0], BlockCodecs::count());
}

}  // namespace pisa
