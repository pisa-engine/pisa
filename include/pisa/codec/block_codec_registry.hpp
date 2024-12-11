#pragma once

#include <algorithm>
#include <memory>
#include <span>
#include <string_view>

#include <fmt/format.h>

#include "codec/block_codec.hpp"

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

/**
 * Resolves a block codec from a name and returns a shared pointer to the created object.
 *
 * If the name is not recognized, `nullptr` is returned.
 */
[[nodiscard]] auto get_block_codec(std::string_view name) -> BlockCodecPtr;

/**
 * Lists the names of all known block codecs.
 */
[[nodiscard]] constexpr auto get_block_codec_names() -> std::span<std::string_view const>;

}  // namespace pisa
