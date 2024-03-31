#include "codec/block_codec_registry.hpp"

#include <algorithm>
#include <array>
#include <memory>
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

template <typename... C>
struct BlockCodecRegistry {
    using BlockCodecConstructor = std::unique_ptr<BlockCodec> (*)();

    constexpr static std::array<std::string_view, sizeof...(C)> names =
        std::array<std::string_view, sizeof...(C)>{C::name...};

    constexpr static std::array<BlockCodecConstructor, sizeof...(C)> constructors =
        std::array<BlockCodecConstructor, sizeof...(C)>{[]() -> std::unique_ptr<BlockCodec> {
            return std::make_unique<C>();
        }...};

    constexpr static auto count() -> std::size_t { return sizeof...(C); }

    static auto get(std::string_view name) -> std::unique_ptr<BlockCodec> {
        auto pos = std::find(names.begin(), names.end(), name);
        if (pos == names.end()) {
            throw std::domain_error(fmt::format("invalid codec: {}", name));
        }
        auto constructor = constructors[std::distance(names.begin(), pos)];
        return constructor();
    }
};

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

auto get_block_codec(std::string_view name) -> std::unique_ptr<BlockCodec> {
    return BlockCodecs::get(name);
}

auto get_block_codec_names() -> gsl::span<std::string_view const> {
    return gsl::make_span<std::string_view const>(&BlockCodecs::names[0], BlockCodecs::count());
}

}  // namespace pisa
