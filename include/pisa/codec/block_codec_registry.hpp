#pragma once

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
    using BlockCodecConstructor = BlockCodecPtr (*)();

    constexpr static std::array<std::string_view, sizeof...(C)> names =
        std::array<std::string_view, sizeof...(C)>{C::name...};

    constexpr static std::array<BlockCodecConstructor, sizeof...(C)> constructors =
        std::array<BlockCodecConstructor, sizeof...(C)>{[]() -> BlockCodecPtr {
            return std::make_shared<C>();
        }...};

    constexpr static auto count() -> std::size_t { return sizeof...(C); }

    static auto get(std::string_view name) -> BlockCodecPtr {
        auto pos = std::find(names.begin(), names.end(), name);
        if (pos == names.end()) {
            return nullptr;
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

[[nodiscard]] auto get_block_codec(std::string_view name) -> BlockCodecPtr {
    return BlockCodecs::get(name);
}

[[nodiscard]] constexpr auto get_block_codec_names() -> gsl::span<std::string_view const> {
    return gsl::make_span<std::string_view const>(&BlockCodecs::names[0], BlockCodecs::count());
}

}  // namespace pisa
